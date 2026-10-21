"""Parse coordinate-style target names used by the single-target DAG."""

import re

from astropy.coordinates import SkyCoord


_IAU_NAME = re.compile(
    r"^(?:ILT)?J(?P<rah>\d{2})(?P<ram>\d{2})(?P<ras>\d{2}(?:\.\d+)?)"
    r"(?P<sign>[+-])(?P<decd>\d{2})(?P<decm>\d{2})"
    r"(?P<decs>\d{2}(?:\.\d+)?)$",
    re.IGNORECASE,
)


def parse_target_coordinates(target_name: str) -> SkyCoord | None:
    """Parse compact IAU names such as ``ILTJ142543.90+335533.4``."""
    match = _IAU_NAME.fullmatch(target_name.strip().replace(" ", ""))
    if not match:
        return None

    ra = f"{match['rah']}h{match['ram']}m{match['ras']}s"
    dec = (
        f"{match['sign']}{match['decd']}d{match['decm']}m"
        f"{match['decs']}s"
    )
    return SkyCoord(f"{ra} {dec}", frame="icrs")
