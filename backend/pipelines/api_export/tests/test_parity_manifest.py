from exporters.parity import get_manifest_contracts, get_manifest_version

REQUIRED_FIELDS = {
    "contract_id",
    "consumer",
    "public_path_or_table",
    "old_supabase_source",
    "current_r2_or_service_source",
    "status",
    "replacement_strategy",
    "owner_exporter",
    "runtime_owner",
}


def test_parity_manifest_version_is_present() -> None:
    assert get_manifest_version() == "2026-04-22"


def test_parity_manifest_contracts_are_classified() -> None:
    contracts = get_manifest_contracts()

    assert contracts
    assert len({contract["contract_id"] for contract in contracts}) == len(contracts)

    for contract in contracts:
        assert contract.keys() >= REQUIRED_FIELDS
        assert contract["status"] in {"covered", "partial", "missing"}

        if contract["status"] == "covered":
            assert contract["current_r2_or_service_source"]
            assert contract["owner_exporter"] != "none"
