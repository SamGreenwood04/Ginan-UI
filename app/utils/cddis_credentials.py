# app/utils/cddis_credentials.py
from __future__ import annotations
import os, platform, stat, shutil
from pathlib import Path
import netrc

URS = "urs.earthdata.nasa.gov"
CDDIS = "cddis.nasa.gov"

def _win_user_home() -> Path:
    return Path(os.environ.get("USERPROFILE", str(Path.home())))

def netrc_candidates() -> tuple[Path, ...]:
    """
    Return possible credential file paths on the local machine:
      - Windows: first return %USERPROFILE%\\.netrc (used by your HTTPS script), then return %USERPROFILE%\\_netrc
      - macOS/Linux: return ~/.netrc
    """
    if platform.system().lower().startswith("win"):
        return (_win_user_home() / ".netrc", _win_user_home() / "_netrc")
    return (Path.home() / ".netrc",)

def _write_text_secure(p: Path, content: str) -> None:
    p.write_text(content, encoding="utf-8")
    if not platform.system().lower().startswith("win"):
        os.chmod(p, stat.S_IRUSR | stat.S_IWUSR)  # 0600

def save_earthdata_credentials(username: str, password: str) -> tuple[Path, ...]:
    """
    Save Earthdata credentials; write entries for two hosts at once (URS + CDDIS).
    - Windows: write both .netrc and _netrc (ensures your HTTPS script can find .netrc, and the toolchain can use _netrc)
    - macOS/Linux: write ~/.netrc and set permission to 600
    Return the list of actual file paths written.
    """
    # need to see if this overrides the existing .netrc file
    content = (
        f"machine {URS}   login {username} password {password}\n"
        f"machine {CDDIS} login {username} password {password}\n"
    )
    written: list[Path] = []
    for p in netrc_candidates():
        # Windows writes two files; *nix writes one file.
        _write_text_secure(p, content)
        written.append(p)
    # For easier lookup by certain libraries
    os.environ["NETRC"] = str(written[0])
    return tuple(written)

def _ensure_windows_mirror() -> None:
    """
    If only _netrc exists without .netrc, automatically create a copy as .netrc on Windows
    to support download_products_https.py (which reads ~/.netrc).
    """
    if not platform.system().lower().startswith("win"):
        return
    dot, under = _win_user_home() / ".netrc", _win_user_home() / "_netrc"
    if under.exists() and not dot.exists():
        try:
            shutil.copyfile(under, dot)
        except Exception:
            pass

def validate_netrc(required=(URS, CDDIS)) -> tuple[bool, str]:
    """
    Validate whether usable credentials exist:
      - Windows: if only _netrc exists, automatically mirror it as .netrc
      - Check that all required hosts have login/password
      - Set the NETRC environment variable to point to the preferred file
        (on Windows: .netrc; on *nix: ~/.netrc)
    Return (ok, path or error reason)
    """
    _ensure_windows_mirror()
    candidates = netrc_candidates()
    # Use the first existing file as the “preferred” one (on Windows: .netrc; on *nix: ~/.netrc)
    p = next((c for c in candidates if c.exists()), candidates[0])
    if not p.exists():
        return False, f"not found: {p}"
    try:
        n = netrc.netrc(p)
        for host in required:
            auth = n.authenticators(host)
            if not auth or not auth[0] or not auth[2]:
                return False, f"missing credentials for {host} in {p}"
        os.environ["NETRC"] = str(p)
        return True, str(p)
    except Exception as e:
        return False, f"invalid netrc {p}: {e}"