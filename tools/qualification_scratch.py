"""Observe the actual disk and private RAM filesystems of an engine worker."""

import os
import re
from pathlib import Path

RAM_DIRECTORY = "start-recovery-ram"
RAM_OPTION = "--qualification-start-recovery-ram"


# Match the resolved path and device to the deepest mount in this process's namespace.
def mount_evidence(directory: Path) -> dict:
    directory = directory.resolve(strict=True)
    device = directory.stat().st_dev
    number = f"{os.major(device)}:{os.minor(device)}"
    mounts = []
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        left, right = line.split(" - ", 1)
        fields = left.split()
        if fields[2] == number:
            mountpoint = Path(re.sub(r"\\([0-7]{3})", lambda match: chr(int(match[1], 8)), fields[4]))
            if directory.is_relative_to(mountpoint):
                mounts.append((mountpoint, right.split()[0]))
    if not mounts:
        raise ValueError("qualification scratch has no visible filesystem mount")
    mountpoint, filesystem = max(mounts, key=lambda row: len(row[0].parts))
    return {"path": str(directory), "device": device, "mountpoint": str(mountpoint), "filesystem": filesystem}


# Refuse a volatile worker default or a RAM path that is not this worker's private tmpfs mount.
def worker_scratch(disk: Path, ram: Path) -> dict:
    disk_record, ram_record = mount_evidence(disk), mount_evidence(ram)
    if (
        disk_record["filesystem"] in ("tmpfs", "ramfs")
        or disk_record["device"] == ram_record["device"]
        or ram_record["filesystem"] != "tmpfs"
        or ram_record["mountpoint"] != ram_record["path"]
        or Path(ram_record["path"]) != Path(disk_record["path"]) / RAM_DIRECTORY
        or ram.stat().st_uid != os.getuid()
        or ram.stat().st_mode & 0o777 != 0o700
    ):
        raise ValueError("engine workers require disk scratch and a separate private Start recovery tmpfs")
    return {"disk": disk_record, "ram": ram_record}


# Record the device of the real path received by the test or of its completed SQLite journal.
def path_evidence(path: Path) -> dict:
    path = path.resolve(strict=True)
    return {"path": str(path), "device": path.stat().st_dev}
