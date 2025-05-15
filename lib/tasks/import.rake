# lib/tasks/import.rake
namespace :import do
  desc "Import transactions from db/transactions.csv to the transactions table"
  task :transactions_from_csv => :environment do
    require 'csv'

    csv_file_path = Rails.root.join('db', 'transactions.csv')
    imported_count = 0
    error_count = 0

    begin
      CSV.foreach(csv_file_path, headers: true) do |row|
        begin
          Transaction.create!( # Using create! to raise ActiveRecord::RecordInvalid on validation errors
            transaction_id: row['Transaction']&.strip,
            date: row['Timestamp']&.strip.present? ? DateTime.parse(row['Timestamp']&.strip) : nil,
            id: row['Customerid']&.strip,
            name: row['Name']&.strip,
            surname: row['Surname']&.strip,
            shipping_state: row['Shipping_State']&.strip,
            item: row['Item']&.strip,
            description: row['Description']&.strip,
            amount: row['Retail_Price']&.strip,
            loyalty_discount: row['Loyalty_Discount']&.strip
          )
          imported_count += 1
          puts "Imported transaction: #{row['Transaction']}"
        rescue ActiveRecord::RecordInvalid => e
          error_count += 1
          puts "Error importing transaction: #{row['Transaction']} - Validation failed: #{e.message}"
        rescue ArgumentError => e
          error_count += 1
          puts "Error importing transaction: #{row['Transaction']} - Argument error: #{e.message} (Likely date parsing issue)"
        rescue StandardError => e
          error_count += 1
          puts "Error importing transaction: #{row['Transaction']} - Other error: #{e.message}"
        end
      end
      puts "Finished importing #{imported_count} transactions with #{error_count} errors from #{csv_file_path}"
    rescue StandardError => e
      puts "Error reading CSV: #{e.message}"
    end
  end
end