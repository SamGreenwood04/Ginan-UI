# app/utils/products_manager.py

import logging
from pathlib import Path
import shutil
from datetime import datetime
from typing import Optional, Dict, Any


def archive_products(products_dir: Path, reason: str = "manual",
                     exclude_patterns: Optional[list[str]] = None) -> Optional[Path]:
    """
    Archive GNSS product files from products_dir into a timestamped subfolder
    under products_dir/archived/.

    :param products_dir: Directory containing GNSS product files
    :param reason: String describing why the archive is happening (e.g., "rinex_change", "ppp_selection_change")
    :param exclude_patterns: Optional list of glob patterns to exclude from archiving
    :return: Path to the archive folder if files were archived, else None
    """
    if not products_dir.exists():
        logging.warning(f"[products_manager] Products dir {products_dir} does not exist.")
        return None

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_dir = products_dir / "archived" / f"{reason}_{timestamp}"
    archive_dir.mkdir(parents=True, exist_ok=True)

    product_patterns = [
        "finals.data.iau2000.txt",  # EOP file
        "BRDC*.rnx*",               # BRDC broadcast nav files
        "*.SP3",                    # precise orbit
        "*.CLK",                    # clock files
        "*.BIA",                    # biases
        "*.ION",                    # ionosphere products (if used)
        "*.TRO",                    # troposphere products (if used)
    ]

    # Remove excluded patterns
    if exclude_patterns:
        product_patterns = [p for p in product_patterns if p not in exclude_patterns]

    archived_files = []
    for pattern in product_patterns:
        for file in products_dir.glob(pattern):
            try:
                target = archive_dir / file.name
                shutil.move(str(file), str(target))
                archived_files.append(file.name)
            except Exception as e:
                logging.warning(f"[products_manager] Failed to archive {file.name}: {e}")

    if archived_files:
        logging.info(f"[products_manager] Archived {len(archived_files)} files → {archive_dir}")
        return archive_dir
    else:
        logging.info("[products_manager] No matching product files found to archive.")
        return None


def archive_products_if_rinex_changed(current_rinex: Path,
                                      last_rinex: Optional[Path],
                                      products_dir: Path) -> Optional[Path]:
    """
    If the RINEX file has changed since last load, archive the cached products.
    """
    if last_rinex and current_rinex.resolve() == last_rinex.resolve():
        logging.info("[products_manager] RINEX file unchanged — skipping product cleanup.")
        return None

    logging.info("[products_manager] RINEX file changed — archiving old products.")
    return archive_products(products_dir, reason="rinex_change")


def archive_products_if_selection_changed(current_selection: Dict[str, Any],
                                          last_selection: Optional[Dict[str, Any]],
                                          products_dir: Path) -> Optional[Path]:
    """
    If the PPP product selection (AC/project/solution) has changed, archive the cached products.
    Excludes BRDC and finals.data.iau2000.txt since they are reusable.
    """
    if last_selection and current_selection == last_selection:
        logging.info("[products_manager] PPP product selection unchanged — skipping product cleanup.")
        return None

    if last_selection:
        diffs = {k: (last_selection.get(k), current_selection.get(k))
                 for k in set(last_selection) | set(current_selection)
                 if last_selection.get(k) != current_selection.get(k)}
        logging.info(f"[products_manager] PPP selection changed → differences: {diffs}")

    return archive_products(
        products_dir,
        reason="ppp_selection_change",
        exclude_patterns=["finals.data.iau2000.txt", "BRDC*.rnx*"]
    )

