require 'google/cloud/bigquery'
require 'json'
# testing
class BigqueryService
  def initialize
    @project_id = Rails.configuration.database_configuration[Rails.env]["project_id"]
    @dataset_name = Rails.configuration.database_configuration[Rails.env]["dataset"]
    @bigquery = Google::Cloud::Bigquery.new(project_id: @project_id)
  end

  def insert_transaction(transaction) # Changed method name and argument
    dataset = @bigquery.dataset @dataset_name
    table = dataset.table "transactions" #  Hardcoded table name, or get from config

    if table
      rows = [{
        "Transaction" => transaction.Transaction,
        "Timestamp" => transaction.Timestamp&.iso8601,
        "CustomerID" => transaction.CustomerID,
        "Name" => transaction.Name,
        "Surname" => transaction.Surname,
        "Shipping_State" => transaction.Shipping_State,
        "Item" => transaction.Item,
        "Description" => transaction.Description,
        "Retail_Price" => transaction.Retail_Price,
        "Loyalty_Discount" => transaction.Loyalty_Discount,
        "created_at" => transaction.created_at&.iso8601,
        "updated_at" => transaction.updated_at&.iso8601
      }]
      begin
        response = table.insert_rows rows
        if response.success?
          puts "Successfully inserted transaction (ID: #{transaction.id}) into BigQuery."
          Rails.logger.info "Successfully inserted transaction (ID: #{transaction.id}) into BigQuery."
        else
          puts "Error inserting transaction (ID: #{transaction.id}) into BigQuery:"
          response.errors.each { |error| puts error.message }
          Rails.logger.error "Error inserting transaction (ID: #{transaction.id}) into BigQuery: #{response.errors.inspect}"
          #  Don't raise here, handle in controller
        end
      rescue Google::Cloud::Bigquery::Error => e
        puts "BigQuery API error: #{e.message}"
        Rails.logger.error "BigQuery API error: #{e.message}"
        #  Don't raise here, handle in controller
      rescue StandardError => e
        puts "An unexpected error occurred: #{e.message}"
        Rails.logger.error "Unexpected error: #{e.message}"
        #  Don't raise here, handle in controller
      end
    else
      puts "BigQuery table 'transactions' not found."
      Rails.logger.error "BigQuery table 'transactions' not found."
      #  Consider creating the table here, or raise an exception
      raise "BigQuery table 'transactions' not found." #  Raise, because table *should* exist.
    end
  end
end