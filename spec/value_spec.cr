require "./spec_helper"

describe AMQP10::Protocol::Value do
  it "keeps values compact enough for inline scalar storage" do
    sizeof(AMQP10::Protocol::Value).should be <= 32
  end
end
