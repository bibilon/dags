import paramiko
import hashlib


def hash_file_on_sftp(hostname, port, username, password, remote_file_path, hash_algorithm='sha256', block_size=65536):
    # Kết nối tới SFTP server
    transport = paramiko.Transport((hostname, port))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)

    # Chọn thuật toán băm
    if hash_algorithm.lower() == 'sha256':
        hasher = hashlib.sha256()
    elif hash_algorithm.lower() == 'md5':
        hasher = hashlib.md5()
    else:
        raise ValueError(f"Unsupported hash algorithm: {hash_algorithm}")

    # Mở tệp tin trên SFTP server và đọc theo từng khối
    with sftp.open(remote_file_path, 'rb') as remote_file:
        while True:
            data = remote_file.read(block_size)
            if not data:
                break
            hasher.update(data)

    # Đóng kết nối SFTP
    sftp.close()
    transport.close()

    # Trả về giá trị băm dưới dạng chuỗi hex
    return hasher.hexdigest()

# Cấu hình kết nối SFTP
hostname = '14.231.238.41'
port = 2223
username = 'nguyen'
password = 'vwefWEHKIer#^&843VDsds'
remote_file_path = '/home/nguyen/thinhdv/data/SHOP.csv.'

# Tính toán hàm băm của tệp tin trên SFTP
hash_value = hash_file_on_sftp(hostname, port, username, password, remote_file_path)
print(f"SHA-256 hash of the file on SFTP: {hash_value}")
