#include "MiniDB.h"

#include <chrono>
#include <cstring>
#include <iostream>
#include <vector>
#include <array>
#include <filesystem>

namespace minidb {

MiniDB::MiniDB(const std::string& db_path) : db_path_(db_path) {
    // Check if main file is missing but a backup exists (crash during Compact)
    std::string backup_path = db_path_ + ".bak";
    std::ifstream check_main(db_path_.c_str());
    if (!check_main.good()) {
        std::ifstream check_bak(backup_path.c_str());
        if (check_bak.good()) {
            check_bak.close();
            std::rename(backup_path.c_str(), db_path_.c_str());
            std::cerr << "Recovered database from backup after crash during compaction.\n";
        } else {
            // Neither exists, create a new file
            std::ofstream create_file(db_path_.c_str(), std::ios::out | std::ios::binary);
            create_file.close();
        }
    }
    check_main.close();

    // Disable C++ level buffering so writes immediately go to the OS page cache.
    // This allows concurrent std::ifstream readers to see the data without explicit flushing.
    data_file_.rdbuf()->pubsetbuf(nullptr, 0);

    // Open file in read/write binary mode
    data_file_.open(db_path_, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    if (!data_file_.is_open()) {
        throw std::runtime_error("Failed to open DB file: " + db_path_);
    }

    // Recover index from file
    if (!Recover()) {
        std::cerr << "Warning: Recovery encountered an error or a corrupt record in " << db_path_ << "\n";
    }

    if (!OpenReadHandle()) {
        throw std::runtime_error("Failed to open read handle for DB file: " + db_path_);
    }
}

MiniDB::~MiniDB() {
    std::unique_lock<std::shared_mutex> lock(db_mutex_);
    CloseReadHandle();
    if (data_file_.is_open()) {
        data_file_.flush();
        data_file_.close();
    }
}

#ifdef _WIN32

bool MiniDB::OpenReadHandle() {
    read_handle_ = CreateFileA(
        db_path_.c_str(),
        GENERIC_READ,
        FILE_SHARE_READ | FILE_SHARE_WRITE,
        NULL,
        OPEN_EXISTING,
        FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OVERLAPPED,
        NULL);
    return read_handle_ != INVALID_HANDLE_VALUE;
}

void MiniDB::CloseReadHandle() {
    if (read_handle_ != INVALID_HANDLE_VALUE) {
        CloseHandle(read_handle_);
        read_handle_ = INVALID_HANDLE_VALUE;
    }
}

bool MiniDB::PRead(void* buf, size_t len, uint64_t offset) {
    thread_local HANDLE tl_event = CreateEvent(NULL, TRUE, FALSE, NULL);

    OVERLAPPED ov = {};
    ov.Offset     = static_cast<DWORD>(offset & 0xFFFFFFFF);
    ov.OffsetHigh = static_cast<DWORD>(offset >> 32);
    ov.hEvent     = tl_event;
    ResetEvent(tl_event);

    DWORD bytes_read = 0;
    BOOL ok = ReadFile(read_handle_, buf, static_cast<DWORD>(len), &bytes_read, &ov);

    if (!ok) {
        DWORD err = GetLastError();
        if (err == ERROR_IO_PENDING) {
            ok = GetOverlappedResult(read_handle_, &ov, &bytes_read, TRUE);
        }
    }

    return ok && bytes_read == static_cast<DWORD>(len);
}

#else

bool MiniDB::OpenReadHandle() {
    read_fd_ = open(db_path_.c_str(), O_RDONLY);
    return read_fd_ >= 0;
}

void MiniDB::CloseReadHandle() {
    if (read_fd_ >= 0) {
        close(read_fd_);
        read_fd_ = -1;
    }
}

bool MiniDB::PRead(void* buf, size_t len, uint64_t offset) {
    char* ptr = static_cast<char*>(buf);
    size_t remaining = len;

    while (remaining > 0) {
        ssize_t bytes_read = pread(read_fd_, ptr, remaining, offset);
        if (bytes_read < 0) {
            if (errno == EINTR) continue;  // Signal interrupted, retry
            return false;                   // Real error
        }
        if (bytes_read == 0) {
            return false;  // Unexpected EOF
        }
        ptr      += bytes_read;
        offset   += bytes_read;
        remaining -= bytes_read;
    }
    return true;
}

#endif


static const std::array<uint32_t, 256>& GetCRC32Table() {
    static const std::array<uint32_t, 256> table = []() {
        std::array<uint32_t, 256> t{};
        for (uint32_t i = 0; i < 256; i++) {
            uint32_t crc = i;
            for (uint32_t j = 0; j < 8; j++) {
                crc = (crc & 1) ? (crc >> 1) ^ 0xEDB88320 : crc >> 1;
            }
            t[i] = crc;
        }
        return t;
    }();
    return table;
}

uint32_t MiniDB::CalculateCRC32(const uint8_t* data, size_t length, uint32_t previous_crc) {
    uint32_t crc = ~previous_crc;
    const auto& table = GetCRC32Table();
    for (size_t i = 0; i < length; ++i) {
        crc = (crc >> 8) ^ table[(crc ^ data[i]) & 0xFF];
    }
    return ~crc;
}

bool MiniDB::Sync() {
    std::unique_lock<std::shared_mutex> lock(db_mutex_);
    if (data_file_.is_open()) {
        data_file_.flush();
        return data_file_.good();
    }
    return false;
}

bool MiniDB::AppendRecord(std::string_view key, std::string_view value, bool is_tombstone, bool sync, std::streampos& out_offset) {
    // Prepare payload
    uint64_t timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();
    uint8_t tombstone = is_tombstone ? 1 : 0;
    uint32_t key_len = static_cast<uint32_t>(key.length());
    uint32_t val_len = static_cast<uint32_t>(value.length());

    // Minimize System Calls and Heap Allocations by using a reusable buffer
    size_t total_size = sizeof(RecordHeader) + key_len + val_len;
    if (write_buffer_.size() < total_size) {
        write_buffer_.resize(total_size);
    }
    
    RecordHeader* header = reinterpret_cast<RecordHeader*>(write_buffer_.data());
    header->magic = MAGIC_BYTES;
    header->timestamp = timestamp;
    header->tombstone = tombstone;
    header->key_len = key_len;
    header->val_len = val_len;

    char* data_ptr = write_buffer_.data() + sizeof(RecordHeader);
    std::memcpy(data_ptr, key.data(), key_len);
    std::memcpy(data_ptr + key_len, value.data(), val_len);

    // Calculate value CRC
    header->value_crc = CalculateCRC32(reinterpret_cast<const uint8_t*>(value.data()), val_len);

    // Calculate header CRC (timestamp + tombstone + key_len + val_len + key string)
    const uint8_t* header_start = reinterpret_cast<const uint8_t*>(&header->timestamp);
    uint32_t crc = CalculateCRC32(header_start, sizeof(uint64_t) + sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint32_t));
    header->header_crc = CalculateCRC32(reinterpret_cast<const uint8_t*>(key.data()), key_len, crc);

    // Seek to end to append
    data_file_.clear();
    data_file_.seekp(0, std::ios::end);
    out_offset = data_file_.tellp();

    // Single write syscall
    data_file_.write(write_buffer_.data(), total_size);
    data_file_.flush();  // Always flush so pread/ReadFile sees the data
    (void)sync; // Suppress unused parameter warning

    return data_file_.good();
}

bool MiniDB::Put(std::string_view key, std::string_view value, bool sync) {
    std::unique_lock<std::shared_mutex> lock(db_mutex_);
    std::streampos offset;
    if (AppendRecord(key, value, false, sync, offset)) {
        key_dir_[std::string(key)] = offset;
        return true;
    }
    return false;
}

bool MiniDB::Delete(std::string_view key, bool sync) {
    std::unique_lock<std::shared_mutex> lock(db_mutex_);
    std::string key_str(key);
    if (key_dir_.find(key_str) == key_dir_.end()) {
        return false; // Key not found
    }

    std::streampos offset;
    if (AppendRecord(key, "", true, sync, offset)) {
        key_dir_.erase(key_str);
        return true;
    }
    
    return false;
}

std::optional<std::string> MiniDB::Get(std::string_view key) {
    std::shared_lock<std::shared_mutex> lock(db_mutex_);

    std::string key_str(key);
    auto it = key_dir_.find(key_str);
    if (it == key_dir_.end()) return std::nullopt;

    uint64_t offset = static_cast<uint64_t>(it->second);

    // Read header via pread/ReadFile
    RecordHeader header;
    if (!PRead(&header, sizeof(RecordHeader), offset)) return std::nullopt;
    if (header.magic != MAGIC_BYTES) return std::nullopt;
    if (header.key_len > 1024 * 1024 || header.val_len > 128 * 1024 * 1024) return std::nullopt;

    // Read value (skip past header + key)
    uint64_t value_offset = offset + sizeof(RecordHeader) + header.key_len;
    std::string value;
    value.resize(header.val_len);
    if (!PRead(&value[0], header.val_len, value_offset)) return std::nullopt;

    // Validate CRC
    uint32_t val_crc = CalculateCRC32(
        reinterpret_cast<const uint8_t*>(value.data()), header.val_len);
    if (val_crc != header.value_crc) return std::nullopt;

    return value;
}

bool MiniDB::Recover() {
    std::unique_lock<std::shared_mutex> lock(db_mutex_);
    data_file_.clear();
    data_file_.seekg(0, std::ios::beg);

    key_dir_.clear();
    bool all_good = true;

    while (true) {
        std::streampos current_offset = data_file_.tellg();
        RecordHeader header;
        
        if (data_file_.peek() == EOF) break;
        if (!data_file_.read(reinterpret_cast<char*>(&header), sizeof(RecordHeader))) {
            // Torn write: incomplete header — truncate at this offset
            all_good = false;
            TruncateAt(static_cast<std::streamoff>(current_offset));
            break;
        }

        if (header.magic != MAGIC_BYTES) {
            all_good = false;
            TruncateAt(static_cast<std::streamoff>(current_offset));
            break;
        }
        if (header.key_len > 1024 * 1024 || header.val_len > 128 * 1024 * 1024) { 
            all_good = false;
            TruncateAt(static_cast<std::streamoff>(current_offset));
            break;
        }

        // Fast Startup Recovery: Only read the key, skip the value
        std::string read_key;
        read_key.resize(header.key_len);
        if (!data_file_.read(&read_key[0], header.key_len)) {
            all_good = false;
            TruncateAt(static_cast<std::streamoff>(current_offset));
            break;
        }

        // Validate Header CRC
        uint32_t crc = CalculateCRC32(reinterpret_cast<const uint8_t*>(&header.timestamp), 
                                      sizeof(uint64_t) + sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint32_t));
        crc = CalculateCRC32(reinterpret_cast<const uint8_t*>(read_key.data()), header.key_len, crc);

        if (crc != header.header_crc) {
            all_good = false;
            TruncateAt(static_cast<std::streamoff>(current_offset));
            break;
        }

        // Skip value bytes on disk! HUGE speedup.
        data_file_.seekg(header.val_len, std::ios::cur);
        if (data_file_.fail()) {
            all_good = false;
            TruncateAt(static_cast<std::streamoff>(current_offset));
            break;
        }

        if (header.tombstone == 1) {
            key_dir_.erase(read_key);
        } else {
            key_dir_[read_key] = current_offset;
        }
    }
    
    data_file_.clear();
    return all_good;
}

bool MiniDB::Compact() {
    std::string compact_file_path = db_path_ + ".compact";
    std::unordered_map<std::string, std::streampos> temp_key_dir;

    std::ofstream compact_file(compact_file_path, std::ios::out | std::ios::binary);
    if (!compact_file.is_open()) return false;

    std::unique_lock<std::shared_mutex> lock(db_mutex_);

    for (const auto& pair : key_dir_) {
        const std::string& key = pair.first;
        std::streampos old_offset = pair.second;

        data_file_.clear();
        data_file_.seekg(old_offset, std::ios::beg);

        RecordHeader header;
        data_file_.read(reinterpret_cast<char*>(&header), sizeof(RecordHeader));
        
        size_t var_len = header.key_len + header.val_len;
        if (write_buffer_.size() < var_len) {
            write_buffer_.resize(var_len);
        }
        data_file_.read(write_buffer_.data(), var_len);
        
        std::streampos new_offset = compact_file.tellp();
        
        compact_file.write(reinterpret_cast<const char*>(&header), sizeof(RecordHeader));
        compact_file.write(write_buffer_.data(), var_len);
        
        temp_key_dir[key] = new_offset;
    }

    compact_file.flush();
    compact_file.close();
    data_file_.close();
    
    CloseReadHandle();

    // More robust atomic rename simulation utilizing backup tracking
    std::string backup_path = db_path_ + ".bak";
    std::remove(backup_path.c_str());
    
    // Rename current to backup
    if (std::rename(db_path_.c_str(), backup_path.c_str()) != 0) {
        return false; // Backup failed, abort
    }

    // Rename compact to current
    if (std::rename(compact_file_path.c_str(), db_path_.c_str()) != 0) {
        // Renaming failed, attempting rollback
        std::rename(backup_path.c_str(), db_path_.c_str());
        return false;
    }

    // Fully succeeded, remove safety backup
    std::remove(backup_path.c_str());

    // Reopen data file
    data_file_.open(db_path_, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    if (!data_file_.is_open()) {
        return false; // Critical failure
    }

    if (!OpenReadHandle()) {
        return false;
    }

    key_dir_ = std::move(temp_key_dir);

    return true;
}

bool MiniDB::TruncateAt(std::streamoff offset) {
    // Close streams before truncation
    data_file_.close();
    CloseReadHandle();

    std::error_code ec;
    std::filesystem::resize_file(db_path_, static_cast<std::uintmax_t>(offset), ec);
    if (ec) {
        std::cerr << "Warning: Failed to truncate database file at offset " << offset << ": " << ec.message() << "\n";
    } else {
        std::cerr << "Recovery: Truncated corrupted trailing bytes at offset " << offset << "\n";
    }

    // Reopen streams
    data_file_.open(db_path_, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    
    OpenReadHandle();

    return !ec;
}

} // namespace minidb
