class CreateBiSyncs < ActiveRecord::Migration[8.0]
  def change
    create_table :bi_syncs do |t|
      t.string :db_name
      t.string :table_name
      t.datetime :synced_started_at
      t.datetime :synced_on
      t.timestamps
    end
  end
end
