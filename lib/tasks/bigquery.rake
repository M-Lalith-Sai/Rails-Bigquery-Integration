# lib/tasks/bigquery.rake
namespace :bigquery do
  desc "Import Transaction data to BigQuery"
  task :import_transactions => :environment do
    require 'google/cloud/bigquery'
    require 'json'
    require_relative '../import_from_model'

    begin
      bigquery_config = Rails.configuration.database_configuration[Rails.env]["bigquery"]
      project_id = bigquery_config["project_id"]
      dataset_name = bigquery_config["dataset"]
      table_name = "transactions"

      bq = Google::Cloud::Bigquery.new(project_id: project_id)
      dataset = bq.dataset dataset_name
      table = dataset.table table_name

      rows_to_insert = fetch_transactions_for_bigquery

      if table && rows_to_insert.present?
        rows_to_insert.each do |row|
          response = table.insert(row)
          if response.success?
            puts "Successfully inserted transaction with ID: #{row['Transaction']}"
            Rails.logger.info "Successfully inserted transaction with ID: #{row['Transaction']}"
          else
            puts "Error inserting transaction with ID: #{row['Transaction']}: #{response.errors.inspect}"
            Rails.logger.error "Error inserting transaction with ID: #{row['Transaction']}: #{response.errors.inspect}"
          end
        end
        puts "Finished importing #{rows_to_insert.count} transactions."
        Rails.logger.info "Finished importing #{rows_to_insert.count} transactions."
      else
        error_message = "BigQuery table '#{table_name}' not found in dataset '#{dataset_name}' or no transactions to import."
        puts error_message
        Rails.logger.error error_message
      end

    rescue Google::Cloud::Bigquery::Error => e
      puts "BigQuery API error: #{e.message}"
      Rails.logger.error "BigQuery API error: #{e.message}"
    rescue StandardError => e
      puts "An unexpected error occurred: #{e.message}"
      Rails.logger.error "An unexpected error occurred: #{e.message}"
    end
  end
end