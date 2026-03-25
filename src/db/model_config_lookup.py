from typing import Dict
from src.db.db_service import DatabaseService


LOOKUP_SCHEMA = "data"
MODEL_TABLE = "ModelDefinition"


def has_model_definition_table(
    db: DatabaseService,
    schema: str = LOOKUP_SCHEMA,
) -> bool:
    rows = db.execute_query(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_name = %s
        ) AS exists_flag
        """,
        (schema, MODEL_TABLE),
    )
    return bool(rows and rows[0]["exists_flag"])


def fetch_model_definition_options(
    db: DatabaseService,
    schema: str = LOOKUP_SCHEMA,
) -> Dict[str, object]:
    rows = db.execute_query(
        f"""
        SELECT
            "ProviderName",
            "ModelName"
        FROM "{schema}"."{MODEL_TABLE}"
        WHERE COALESCE("IsActive", TRUE) = TRUE
        ORDER BY "ProviderName", "SortOrder", "Id"
        """
    )

    providers: list[str] = []
    models_by_provider: Dict[str, list[str]] = {}
    for row in rows:
        provider_name = row["ProviderName"]
        model_name = row["ModelName"]

        if provider_name not in models_by_provider:
            providers.append(provider_name)
            models_by_provider[provider_name] = []

        models_by_provider[provider_name].append(model_name)

    return {
        "providers": providers,
        "models_by_provider": models_by_provider,
    }


def is_valid_model_definition(
    db: DatabaseService,
    provider: str,
    model_name: str,
    schema: str = LOOKUP_SCHEMA,
) -> bool:
    rows = db.execute_query(
        f"""
        SELECT EXISTS (
            SELECT 1
            FROM "{schema}"."{MODEL_TABLE}"
            WHERE "ProviderName" = %s
              AND "ModelName" = %s
              AND COALESCE("IsActive", TRUE) = TRUE
        ) AS exists_flag
        """,
        (provider, model_name),
    )
    return bool(rows and rows[0]["exists_flag"])
