CREATE TABLE FEATURE_REGISTRY (
  NAME VARCHAR,
  VERSION VARCHAR,
  READINESS VARCHAR DEFAULT 'DRAFT',
  TILE_SPECS VARIANT,
  IS_DEFAULT BOOLEAN DEFAULT True,
  ONLINE_ENABLED BOOLEAN DEFAULT False,
  CREATED_AT TIMESTAMP DEFAULT SYSDATE()
);
