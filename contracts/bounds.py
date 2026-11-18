"""Physical bounds of the simulated city. Single source for the simulator and the rules."""

from __future__ import annotations

from dataclasses import dataclass

MAX_SPEED_KMH = 80.0


@dataclass(frozen=True)
class BBox:
    lat_min: float
    lat_max: float
    lon_min: float
    lon_max: float

    def contains(self, lat: float, lon: float) -> bool:
        return self.lat_min <= lat <= self.lat_max and self.lon_min <= lon <= self.lon_max


HCMC_BBOX = BBox(lat_min=10.65, lat_max=10.90, lon_min=106.55, lon_max=106.85)
