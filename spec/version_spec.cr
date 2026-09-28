require "./spec_helper"

describe AMQP10::Protocol::VERSION do
  it "matches the shard version" do
    AMQP10::Protocol::VERSION.should eq {{ `shards version #{__DIR__}/..`.chomp.stringify }}
  end
end
