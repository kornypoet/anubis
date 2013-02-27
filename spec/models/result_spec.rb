require 'spec_helper'

describe Anubis::Result do

  let(:db_result) do
    {
      'my:row:key' => {
        'column:qualifier'         => [
          { value: 'some_val',    timestamp: 1361985404491 },
          { value: 'other_val',   timestamp: 1361985391553 }
        ],
        'another_col:another_qual' => [
          { value: 'another_val', timestamp: 1361985412741 }
        ]
      }
    }
  end

  let(:test_op) { Anubis::Operation.new('my_table', ['column:qualifier', 'another_col:another_val'], ['my:row:key']) }

  subject { described_class.new(test_op, :get, db_result) }

  context '#[]' do
    it 'returns the first cell version' do
      subject['column:qualifier'].raw.should eq({ value: 'some_val', timestamp: 1361985404491 })
    end
  end

  context '#raw' do
    it 'returns the raw result as a data structure' do
      subject.raw.should eq(db_result)
    end
  end

  context '#columns' do
    it 'returns the columns that correspond to this result' do
      subject.columns.should eq(['column:qualifier', 'another_col:another_qual'])
    end
  end

  context '#operation' do
    it 'returns the operation that generated the result' do
      subject.operation.should eq(test_op)
    end
  end

  context '#operation_method' do
    it 'returns the operation method that generated the result' do
      subject.operation_method.should eq(:get)
    end
  end
  
  context '#each_version' do
    context 'given a block' do      
      let(:iterator){ ->(cell, cursor){ } }

      it 'executes the block for each version' do
        iterator.should_receive(:call).exactly(2).times
        subject.each_version('column:qualifier', &iterator)
      end
    end

    context 'without a block' do
      it 'returns an enumerable object for iteration' do
        subject.each_version('column:qualifier').should be_instance_of(Anubis::CellCollection)
      end      
    end
  end  
end

describe Anubis::CellCollection do

  subject do
    described_class.new([
      { value: 'some_val',  timestamp: 1361985404491 },
      { value: 'other_val', timestamp: 1361985391553 }
    ])
  end
  
  context '#next' do
    it 'returns the current version of a cell' do
      subject.next.raw.should eq({ value: 'some_val',  timestamp: 1361985404491 })
      subject.next.raw.should eq({ value: 'other_val', timestamp: 1361985391553 })
      subject.next.should     eq(nil)
    end
  end
  
  context '#rewind' do
    it 'resets the version cursor for reading' do
      subject.next.raw.should eq({ value: 'some_val',  timestamp: 1361985404491 })
      subject.rewind
      subject.next.raw.should eq({ value: 'some_val',  timestamp: 1361985404491 })
    end
  end
  
  context '#each' do
    context 'given a block' do
      let(:iterator){ ->(cell, cursor){ } }
      
      it 'executes the block for each version' do
        yielded = {}
        subject.each do |cell, pos|
          yielded[pos] = cell.raw
        end
        yielded[0].should eq({ value: 'some_val',  timestamp: 1361985404491 })
        yielded[1].should eq({ value: 'other_val', timestamp: 1361985391553 })
      end

      it 'resets the cursor' do
        subject.next
        expect{ subject.each(&iterator) }.to change{ subject.position }.from(1).to(0)
      end
    end

    context 'without a block' do
      it 'does not change the cursor' do
        subject.should_not_receive(:increment_cursor)
        subject.should_not_receive(:reset_cursor)
        subject.each
      end
      
      it 'returns self' do
        subject.each.should eq(subject)
      end
    end
  end
end

describe Anubis::Cell do

  subject{ described_class.new(value: 'foo', timestamp: 123) }

  its(:value)    { should eq('foo') }
  its(:timestamp){ should eq(123)   }

  context '#raw' do
    it 'returns a hash containing the raw data' do
      subject.raw.should eq({ value: 'foo', timestamp: 123 })
    end
  end
end
