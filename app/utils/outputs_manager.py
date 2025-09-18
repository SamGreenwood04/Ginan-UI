# app/utils/outputs_manager.py

import shutil
from pathlib import Path
from datetime import datetime

def archive_old_outputs(output_dir: Path, visual_dir: Path = None):
    """
    Moves existing output files to an archive directory to keep the workspace clean.
    
    :param output_dir: Path to the user-selected output directory.
    :param visual_dir: Optional path to associated visualisation directory.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_dir = output_dir / "archive" / timestamp
    archive_dir.mkdir(parents=True, exist_ok=True)

    # Move .pos, .log, .txt, etc. from output_dir
    moved_files = 0
    for ext in [".pos", ".POS", ".log", ".txt", ".json"]:
        for file in output_dir.glob(f"*{ext}"):
            shutil.move(str(file), archive_dir / file.name)
            moved_files += 1

    # Move HTML visual files (optional)
    if visual_dir and visual_dir.exists():
        visual_archive = archive_dir / "visual"
        visual_archive.mkdir(parents=True, exist_ok=True)
        for html_file in visual_dir.glob("*.html"):
            shutil.move(str(html_file), visual_archive / html_file.name)
            moved_files += 1

    if moved_files > 0:
        print(f"📦 Archived {moved_files} old output file(s) to: {archive_dir}")
    else:
        print("📂 No previous outputs found to archive.")

