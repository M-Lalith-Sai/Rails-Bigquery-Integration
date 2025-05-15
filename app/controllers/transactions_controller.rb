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
    @transactions << new_transaction
    flash[:notice] = "Transaction created!"
    redirect_to transactions_path
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
