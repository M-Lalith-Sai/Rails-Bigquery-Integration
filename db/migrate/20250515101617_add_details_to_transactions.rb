class AddDetailsToTransactions < ActiveRecord::Migration[8.0]
  def change
    add_column :transactions, :name, :string
    add_column :transactions, :surname, :string
    add_column :transactions, :shipping_state, :string
    add_column :transactions, :description, :string
    add_column :transactions, :loyalty_discount, :decimal
  end
end
