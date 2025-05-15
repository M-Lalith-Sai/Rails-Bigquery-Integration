require 'csv'

class TransactionsController < ApplicationController
  before_action :load_transactions
  before_action :set_transaction, only: [:show, :edit, :update, :destroy]

  def index
    if params[:search].present?
      @transactions = @transactions.select do |transaction|
        transaction.any? { |key, value| value.to_s.downcase.include?(params[:search].to_s.downcase) }
      end
    end
  end

  def show
  end

  def new
    @transaction = {}
  end

  def create
    new_transaction = params[:transaction].permit!.to_h

    # Add this back
    @transactions << new_transaction

    # Attempt to insert into BigQuery
    begin
      bigquery_service = BigqueryService.new
      bigquery_service.insert_record(
        "transactions_v2",  # Use your BigQuery table name
        {
          "Transaction" => new_transaction["Transaction"],
          "Timestamp" => new_transaction["Timestamp"],
          "CustomerID" => new_transaction["CustomerID"],
          "Name" => new_transaction["Name"],
          "Surname" => new_transaction["Surname"],
          "Shipping_State" => new_transaction["Shipping_State"],
          "Item" => new_transaction["Item"],
          "Description" => new_transaction["Description"],
          "Retail_Price" => new_transaction["Retail_Price"],
          "Loyalty_Discount" => new_transaction["Loyalty_Discount"],
          "created_at" => Time.now.iso8601, # Add created_at and updated_at
          "updated_at" => Time.now.iso8601
        }
      )
      flash[:notice] = "Transaction created and sent to BigQuery!" # changed flash message
      redirect_to transactions_path
    rescue => e
      puts e
      flash[:error] = "Transaction created, but failed to insert into BigQuery: #{e.message}"
      Rails.logger.error "Error inserting into BigQuery: #{e.message}"
      redirect_to transactions_path # Consider redirecting to a different path
    end
  end

  def edit
  end

  def update
    if @transaction.present?
      @transaction.merge!(params[:transaction].permit!.to_h)
      flash[:notice] = "Transaction updated!"
      redirect_to transaction_path(@transaction['Transaction'])
    else
      flash[:alert] = "Transaction not found."
      redirect_to transactions_path
    end
  end

  def destroy
    @transactions.reject! { |t| t['Transaction'] == params[:id] }
    flash[:notice] = "Transaction deleted!"
    redirect_to transactions_path
  end

  private

  def load_transactions
    @transactions = []
    CSV.foreach(Rails.root.join('db', 'transactions.csv'), headers: true) do |row|
      @transactions << row.to_h
    end
  end

  def set_transaction
    @transaction = @transactions.find { |t| t['Transaction'] == params[:id] }
    if @transaction.nil?
      flash[:alert] = "Transaction not found."
      redirect_to transactions_path
    end
  end
end
