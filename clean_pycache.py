import os
import shutil

def clean_pycache_and_pyc_files(root_dir='.'):
    for dirpath, dirnames, filenames in os.walk(root_dir):
        # ลบโฟลเดอร์ __pycache__
        if '__pycache__' in dirnames:
            pycache_path = os.path.join(dirpath, '__pycache__')
            print(f"ลบโฟลเดอร์: {pycache_path}")
            shutil.rmtree(pycache_path)
            # เอา __pycache__ ออกจาก list เพื่อไม่ให้ os.walk ลงไปซ้ำ
            dirnames.remove('__pycache__')

        # ลบไฟล์ .pyc
        for filename in filenames:
            if filename.endswith('.pyc'):
                file_path = os.path.join(dirpath, filename)
                print(f"ลบไฟล์: {file_path}")
                os.remove(file_path)

if __name__ == '__main__':
    clean_pycache_and_pyc_files()
    print("ลบไฟล์ .pyc และโฟลเดอร์ __pycache__ เรียบร้อยแล้ว")
