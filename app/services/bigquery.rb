require "google/cloud/bigquery"

# Bigquery.new.setup
class Bigquery
  attr_accessor :bqc, :bq_config
  def initialize
    super
    ENV["GOOGLE_APPLICATION_CREDENTIALS"] = "config/google-bigquery-keys.json"
    @bq_config = Rails.configuration.database_configuration[Rails.env]["bigquery"]
    @bqc = Google::Cloud::Bigquery.new(project_id: bq_config["project_id"])
  end

  def setup
    dataset_name = bq_config["dataset"]
    dataset = @bqc.dataset(dataset_name)
    dataset = @bqc.create_dataset(dataset_name) if dataset.blank?
  end

  # TODOs
  # Bigquery.new.setup_table(Transaction)
  def setup_table(model_class, partition_timestamp_column = "created_at")
    db_name = model_class.connection_db_config.database
    table_name = model_class.table_name
    bq_table_name = "#{db_name.gsub("/", ".")}.#{table_name}".gsub(".", "-")
    dataset_name = bq_config["dataset"]

    dataset = @bqc.dataset(dataset_name)
    table = dataset.table(bq_table_name)
    table.delete unless table.blank?

    schema_json = generate_bigquery_schema(model_class)
    table = dataset.create_table(bq_table_name) do |t|
      t.schema do |schema|
        schema.load(schema_json)
      end
    end
    table
  end

  def verify_table_exists(model_class)
    db_name = model_class.connection_db_config.database
    table_name = model_class.table_name
    bq_table_name = "#{db_name.gsub("/", ".")}.#{table_name}".gsub(".", "-")
    dataset_name = bq_config["dataset"]

    dataset = @bqc.dataset(dataset_name)
    table = dataset.table(bq_table_name)
    raise "Table #{bq_table_name} is not created in BiqQuery for the #{db_name} #{table_name}" unless table.blank?
  end

  def load_full_data(model_class)
    verify_table_exists(model_class)

    db_name = model_class.connection_db_config.database
    table_name = model_class.table_name

    start_time = Time.now
    # load full data
    data = []
    model_class.all.each do |record|
      data << record.attributes
    end
    # Insert the data
    # TODO
    end_time = Time.now
    BiSync.sync_update(db_name, table_name, start_time, end_time)
  end

  def load_changes(model_class)
    verify_table_exists(model_class)

    db_name = model_class.connection_db_config.database
    table_name = model_class.table_name
    last_synced_on = BiSync.where(db_name: db_name, table_name: table_name).first.synced_on

    start_time = Time.now
    # load changes
    data = []
    model_class.where([ "updated_at >= ?", last_synced_on ]).each do |record|
      data << record.attributes
    end
    # Upsert the data
    # TODO

    end_time = Time.now
    BiSync.sync_update(db_name, table_name, start_time, end_time)
  end


  private

  def generate_bigquery_schema(model_class)
    schema = model_class.columns.map do |col|
      {
        name: col.name,
        type: rails_type_to_bq(col.type),
        mode: col.null ? "NULLABLE" : "REQUIRED"
      }
    end
    JSON.pretty_generate(schema)
  end

  def rails_type_to_bq(type)
    {
      string:   "STRING",
      text:     "STRING",
      integer:  "INTEGER",
      bigint:   "INTEGER",
      float:    "FLOAT",
      decimal:  "NUMERIC",
      boolean:  "BOOLEAN",
      datetime: "TIMESTAMP",
      date:     "DATE",
      json:     "RECORD",
      jsonb:    "RECORD"
    }[type.to_sym] || "STRING"
  end

  # #
  # def upsert(rows, table_name, primary_key_name, schema_json = nil, partition_timestamp_column = "created_at",
  #               clustering_fields = [ "account_id" ], data_set_name = "bitsila_dl")
  #   staging_table_name = "__staging_#{table_name}"
  #   main_table_id = "#{AppConfig.datalake['bigquery_project_id']}.#{data_set_name}.#{table_name}"
  #   staging_table_id = "#{AppConfig.datalake['bigquery_project_id']}.#{data_set_name}.#{staging_table_name}"

  #   dataset = @bqc.dataset(data_set_name)
  #   dataset = @bqc.create_dataset(data_set_name) if dataset.blank?

  #   staging_table = dataset.table(staging_table_name)
  #   staging_table.delete unless staging_table.blank?

  #   data_file_name = generate_temp_json_file(rows)

  #   staging_load_result = false
  #   if schema_json.present?
  #     staging_table = dataset.create_table(staging_table_name) do |t|
  #       t.schema do |schema|
  #         schema.load(File.read(schema_json))
  #       end
  #     end
  #     staging_load_result = staging_table.load(data_file_name, format: "json")
  #   else
  #     staging_load_result = dataset.load(staging_table_name, data_file_name, format: "json", autodetect: true)
  #     staging_table = dataset.table(staging_table_name) if staging_load_result
  #   end

  #   unless staging_load_result
  #     File.delete(data_file_name)
  #     raise "Staging table load failed"
  #   end

  #   schema_file_name = "#{data_file_name}_schema.json"
  #   staging_table.schema.dump(schema_file_name) if schema_json.blank?

  #   target_table = dataset.table(table_name)
  #   if target_table.blank?
  #     dataset.create_table(table_name) do |t|
  #       t.time_partitioning_type  = "DAY"
  #       t.time_partitioning_field = partition_timestamp_column
  #       t.clustering_fields = clustering_fields
  #       t.schema do |schema|
  #         if schema_json.present?
  #           schema.load(File.read(schema_json))
  #         else
  #           schema.load(File.read(schema_file_name))
  #         end
  #       end
  #     end
  #   else
  #     target_table.schema do |schema|
  #       if schema_json.present?
  #         schema.load(File.read(schema_json))
  #       else
  #         schema.load(File.read(schema_file_name))
  #       end
  #     end
  #   end

  #   # Generate dynamic columns for upsert query
  #   set_columns = rows.first.keys.map { |col| "#{col} = source.#{col}" }.join(", ")
  #   insert_columns = rows.first.keys.join(", ")

  #   # Generate upsert SQL using dynamic columns
  #   upsert_sql = <<~SQL
  #     MERGE #{main_table_id} AS target
  #     USING #{staging_table_id} AS source
  #     ON target.#{primary_key_name} = source.#{primary_key_name}
  #     WHEN MATCHED THEN
  #       UPDATE SET #{set_columns}
  #     WHEN NOT MATCHED THEN
  #       INSERT (#{insert_columns}) VALUES (#{rows.first.keys.map { |col| "source.#{col}" }.join(', ')})
  #   SQL

  #   puts upsert_sql

  #   # Run Query
  #   @bqc.query(upsert_sql)

  #   # Delete the temporary staging table when done
  #   File.delete(schema_file_name) if schema_json.blank?
  #   File.delete(data_file_name)
  #   staging_table.delete
  # end

  # private

  # def generate_temp_json_file(data)
  #   jsonl_data = data.map(&:to_json).join("\n")
  #   jsonl_filename = "/tmp/#{SecureRandom.uuid}.jsonl"
  #   File.open(jsonl_filename, "w") do |file|
  #     file.write(jsonl_data)
  #   end
  #   jsonl_filename
  # end
end
