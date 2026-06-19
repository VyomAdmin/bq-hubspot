from typing import Any


def build_hubspot_properties(
    bq_properties: dict[str, Any],
    object_type: str,
    mappings: dict,
) -> dict[str, Any]:
    """
    Translates BigQuery column names to HubSpot property names using the
    mappings config. Skips nulls and unmapped keys silently.
    """
    type_map: dict[str, str] = mappings.get(object_type, {})
    result: dict[str, Any] = {}
    for bq_col, value in bq_properties.items():
        hs_prop = type_map.get(bq_col)
        if hs_prop is None or value is None:
            continue
        # HubSpot accepts strings for most properties; pass numerics as-is
        result[hs_prop] = value if isinstance(value, (bool, int, float)) else str(value)
    return result


def build_batch_payload(
    rows: list[dict],
    object_type: str,
    mappings: dict,
) -> list[dict]:
    """
    Converts a list of BQ rows into enriched dicts carrying both the HubSpot
    payload (ready to POST) and the source row_id (for status writeback).

    Each output item: {"row_id": str, "hs_id": str, "payload": dict}

    Supports:
      - id-based lookup  (hubspot_id is set)
      - property-based lookup (business_key + business_key_property set)
    Rows that can't be identified or produce an empty property set are skipped.
    """
    inputs = []
    for row in rows:
        hs_props = build_hubspot_properties(row["properties"], object_type, mappings)
        if not hs_props:
            continue

        item: dict[str, Any] = {"properties": hs_props}
        hs_id: str | None = None

        if row.get("hubspot_id"):
            item["id"] = str(row["hubspot_id"])
            hs_id = item["id"]
        elif row.get("business_key") and row.get("business_key_property"):
            item["idProperty"] = row["business_key_property"]
            item["id"] = str(row["business_key"])
            hs_id = item["id"]
        else:
            continue  # cannot identify the record

        inputs.append({"row_id": row["row_id"], "hs_id": hs_id, "payload": item})

    return inputs
