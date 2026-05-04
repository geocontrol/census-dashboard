import json
import logging
import os
from typing import Optional

import anthropic
from pydantic import BaseModel

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a query parser for a UK census data dashboard.
Convert natural language queries into a structured JSON filter specification.

Available datasets (id → label, unit, category):
{catalog_json}

Rules:
1. Map each user condition to a filter using the dataset IDs above.
2. "aged 45+" or "over 45" or "population over the age of 45" → use a COMPUTED filter summing age_45_64 + age_65_plus.
3. "elderly" or "older population" → age_65_plus simple filter.
4. If a threshold is vague ("high", "low", "very high", "above average", "significant", "large"), set threshold to null and vague_term to the exact word(s) used.
5. If a threshold is explicit ("over 30%", "more than 25", "above 40"), set threshold to that number and vague_term to null.
6. Operator rules: "high"/"above"/"over"/"more than" → ">"; "low"/"below"/"under"/"less than" → "<".
7. All conditions are AND-combined (an area must meet ALL criteria).
8. Set clarification_needed to true if ANY threshold is null.
9. If a term cannot be mapped to any dataset, add it to unrecognised_terms.
10. Return ONLY valid JSON matching the schema below. No prose, no markdown fences, no explanation.

Output schema (strict):
{
  "filters": [
    {"dataset_id": "<id>", "operator": ">"|"<"|">="|"<=", "threshold": <number>|null, "label": "<human label>", "vague_term": "<word>"|null}
  ],
  "computed_filters": [
    {"operation": "sum", "datasets": ["<id1>", "<id2>"], "operator": ">"|"<"|">="|"<=", "threshold": <number>|null, "label": "<human label>", "vague_term": "<word>"|null}
  ],
  "parsed_summary": "<one sentence describing what the query finds>",
  "clarification_needed": true|false,
  "unrecognised_terms": ["<term>"]
}"""


class FilterSpec(BaseModel):
    dataset_id: str
    operator: str
    threshold: Optional[float] = None
    label: str
    vague_term: Optional[str] = None


class ComputedFilterSpec(BaseModel):
    operation: str
    datasets: list[str]
    operator: str
    threshold: Optional[float] = None
    label: str
    vague_term: Optional[str] = None


class ParsedQuery(BaseModel):
    filters: list[FilterSpec]
    computed_filters: list[ComputedFilterSpec]
    parsed_summary: str
    clarification_needed: bool
    unrecognised_terms: list[str]


def _get_client() -> anthropic.AsyncAnthropic:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY environment variable is not set")
    return anthropic.AsyncAnthropic(api_key=api_key)


async def parse_nl_query(query: str, catalog: dict[str, dict]) -> ParsedQuery:
    catalog_json = json.dumps(
        {k: {"label": v["label"], "unit": v["unit"], "category": v.get("category", "")}
         for k, v in catalog.items()},
        separators=(",", ":"),
    )

    client = _get_client()
    response = await client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        system=[
            {
                "type": "text",
                "text": SYSTEM_PROMPT.format(catalog_json=catalog_json),
                "cache_control": {"type": "ephemeral"},
            }
        ],
        messages=[{"role": "user", "content": query}],
        timeout=30.0,
    )

    raw = response.content[0].text.strip()

    # Strip any accidental markdown fences
    if raw.startswith("```"):
        parts = raw.split("```")
        raw = parts[1] if len(parts) > 1 else raw
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"Claude returned invalid JSON: {e}\nRaw: {raw[:400]}")

    # Validate all dataset_ids exist in catalog
    known_ids = set(catalog.keys())
    for f in parsed.get("filters", []):
        if f.get("dataset_id") not in known_ids:
            raise ValueError(f"Unknown dataset_id from Claude: {f.get('dataset_id')!r}")
    for cf in parsed.get("computed_filters", []):
        for ds_id in cf.get("datasets", []):
            if ds_id not in known_ids:
                raise ValueError(f"Unknown dataset_id in computed filter: {ds_id!r}")

    return ParsedQuery(**parsed)
