class ChangeItemToIntegerInTransactions < ActiveRecord::Migration[8.0]
  def change
    change_column :transactions, :item, :integer
  end
end