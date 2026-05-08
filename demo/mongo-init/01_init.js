// =============================================================================
// MongoDB init — create the checkpoints collection with an index on dag_id
// =============================================================================

db = db.getSiblingDB("config");

db.createCollection("checkpoints");

db.checkpoints.createIndex(
  { dag_id: 1 },
  { unique: true, name: "idx_dag_id" }
);

print("MongoDB: checkpoints collection and index created");
