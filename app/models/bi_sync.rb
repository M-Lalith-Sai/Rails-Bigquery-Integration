class BiSync < ApplicationRecord
  # Assuming you have a database table named 'bi_syncs' with the following columns:
  # - db_name (string)
  # - table_name (string)
  # - synced_started_at (datetime)
  # - synced_on (datetime)

  validates :db_name, presence: true
  validates :table_name, presence: true
  validates :synced_started_at, presence: true
  validates :synced_on, presence: true

  def self.sync_update(db_name, table_name, synced_started_at, synced_on)
    # Ensure db_name and table_name are present
    raise ArgumentError, "db_name and table_name must be provided" if db_name.blank? || table_name.blank?

    bi_sync = BiSync.find_by(db_name: db_name, table_name: table_name)
    if bi_sync
      bi_sync.update(synced_started_at: synced_started_at, synced_on: synced_on)
    else
      BiSync.create(db_name: db_name, table_name: table_name, synced_on: synced_on)
    end
  end
end
