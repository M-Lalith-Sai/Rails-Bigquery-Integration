class Datalake
  SYNC_MODELS = [ Transaction ].freeze

  MODELS_SKIP_ATTRIBUTES = {
    "Transaction" => %w[created_at updated_at]
  }

  def self.sync_all_models_full_data
    SYNC_MODELS.each do |model_class|
      begin
        Bigquery.new.load_full_data(model_class)
      rescue StandardError => e
        Rails.logger.error "Error syncing #{model_class}: #{e.message}"
      end
    end
  end

  def self.sync_all_models_changes
    SYNC_MODELS.each do |model_class|
      begin
        Bigquery.new.load_changes(model_class)
      rescue StandardError => e
        Rails.logger.error "Error syncing #{model_class}: #{e.message}"
      end
    end
  end
end
