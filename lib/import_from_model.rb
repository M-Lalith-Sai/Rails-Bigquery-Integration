# lib/import_from_model.rb
def fetch_transactions_for_bigquery
  Transaction.find_each.map do |transaction|
    {
      "Transaction" => transaction.transaction_id,
      "Timestamp" => transaction.date&.iso8601,
      "CustomerID" => transaction.id, # VERIFY THIS MAPPING!
      "Name" => transaction.name,       # Ensure 'name' column exists in your Rails DB
      "Surname" => transaction.surname,    # Ensure 'surname' column exists
      "Shipping_State" => transaction.shipping_state, # Ensure 'shipping_state' exists
      "Item" => transaction.item.to_s,
      "Description" => transaction.description,    # Ensure 'description' exists
      "Retail_Price" => transaction.amount,
      "Loyalty_Discount" => transaction.loyalty_discount # Ensure 'loyalty_discount' exists
    }
  end
end