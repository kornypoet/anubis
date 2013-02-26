require 'spec_helper'

describe Anubis::Result do

  let(:db_result) do
    {
      'my:row:key' => {
        'column:qualifier'         => [
          { value: 'some_val',  timestamp: Time.now }
        ],
        'another_col:another_qual' => [
          { value: 'other_val', timestamp: Time.now }
        ]
      }
    }
  end

  let(:test_op) { Anubis::Operation.new('my_table', ['column:qualifier', 'another_col:another_val'], ['my:row:key']) }

  subject { described_class.new(op: test_op, raw: db_result) }

  context '#[]' do
    it 'returns the corresponding cell given a column' do
    end
  end

  context '#raw' do
    it 'returns the raw result as a data structure' do
      subject.raw.should eq(db_result)
    end
  end

  context '#columns' do
    it 'returns the columns that correspond to this result' do
    end
  end

  context '#operation' do
    it 'returns the operation that generated the result' do
    end
  end

  context '#operation_method' do
    it 'returns the operation method that generated the result' do
    end
  end
  
  context '#each_version' do
    it 'returns an enumerable object for iteration' do
    end
  end
  
end
