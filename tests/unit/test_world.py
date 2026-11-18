"""World seed data: depots and vans (single source of truth for the order DB seed)."""

from collections import Counter
from pathlib import Path

from simulator.fleet import HCMC_BBOX
from simulator.world import DEPOTS, VANS, seed_sql

SEED_FILE = Path(__file__).resolve().parents[2] / "infra/local/postgres/init/002_seed.sql"


def test_four_depots_inside_hcmc():
    assert len(DEPOTS) == 4
    assert len({d.depot_id for d in DEPOTS}) == 4
    for d in DEPOTS:
        assert HCMC_BBOX.lat_min <= d.lat <= HCMC_BBOX.lat_max
        assert HCMC_BBOX.lon_min <= d.lon <= HCMC_BBOX.lon_max


def test_200_vans_with_gps_ids_50_per_depot():
    assert [v.van_id for v in VANS] == [f"VAN-{i:04d}" for i in range(1, 201)]
    assert Counter(v.depot_id for v in VANS) == {d.depot_id: 50 for d in DEPOTS}
    assert len({v.plate for v in VANS}) == 200


def test_seed_sql_file_matches_world():
    """Regenerate: uv run python -m simulator seed-sql > infra/local/postgres/init/002_seed.sql"""
    assert SEED_FILE.read_text() == seed_sql()


def test_seed_sql_is_idempotent():
    sql = seed_sql()
    assert sql.count("ON CONFLICT DO NOTHING") == 2
