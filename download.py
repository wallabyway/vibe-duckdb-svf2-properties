import requests
import zipfile
import os
import lzma

URN = "dXJuOmFkc2sub2JqZWN0czpvcy5vYmplY3Q6YXBzLWNsaWVudGlkLWJ1Y2tldDIvaWNlLXN0YWRpdW0ubndk"
BASE_URL = f"https://cdn.derivative.autodesk.com/modeldata/file/urn%3Aadsk.fluent%3Afs.file%3Aautodesk-360-translation-storage-prod%2F{URN}%2Foutput%2Fotg_files%2F0%2Foutput%2F0%2F"
MANIFEST_FILENAME = "otg_model.json"
MANIFEST_URL = f"{BASE_URL}{MANIFEST_FILENAME}?acmsession={URN}"
TOKEN_URL = "https://viewer.aps-autodesk.com/api/token"

def get_token():
    return requests.get(TOKEN_URL, headers={"Accept": "*/*"}).json()["access_token"]

def download_file(url, filename, token, folder):
    r = requests.get(url, headers={"Accept": "*/*", "Authorization": f"Bearer {token}"}, stream=True)
    r.raise_for_status()
    path = os.path.join(folder, filename)
    with open(path, "wb") as f:
        for chunk in r.iter_content(8192):
            f.write(chunk)
    if filename == "avs.pack":
        try:
            with zipfile.ZipFile(path) as z:
                z.extractall(os.path.join(folder, filename + ".unzipped"))
        except zipfile.BadZipFile:
            # Try LZMA decompress
            try:
                with open(path, "rb") as fin:
                    data = fin.read()
                    decompressed = lzma.decompress(data)
                out_path = os.path.join(folder, filename + ".decompressed")
                with open(out_path, "wb") as fout:
                    fout.write(decompressed)
            except Exception:
                pass

def main():
    download_dir = 'otg'
    os.makedirs(download_dir, exist_ok=True)
    t = get_token()
    download_file(MANIFEST_URL, MANIFEST_FILENAME, t, download_dir)
    pdb = requests.get(MANIFEST_URL, headers={"Authorization": f"Bearer {t}"}).json()["manifest"]["assets"]["pdb"]
    for f in pdb.values():
        if f:
            download_file(f"{BASE_URL}{f}?acmsession={URN}", f, t, download_dir)

if __name__ == "__main__":
    main()
