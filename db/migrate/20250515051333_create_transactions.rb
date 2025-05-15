class CreateTransactions < ActiveRecord::Migration[8.0]
  def change
    create_table :transactions do |t|
      t.integer :transaction_id
      t.decimal :amount
      t.date :date

      t.timestamps
    end
  end
end
