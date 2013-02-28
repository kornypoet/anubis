require 'spec_helper'

class StubClient

  def getTableNames
    %w[ foo bar baz ]    
  end
  
  def getColumnDescriptors table
    { 
      'foo' => { 'foo' => Anubis::ColumnDetails.new(name: 'qix') },
      'bar' => { 'bar' => Anubis::ColumnDetails.new(name: 'qax') },
      'baz' => { 'baz' => Anubis::ColumnDetails.new(name: 'qux') },
    }.fetch(table)
  end

  def createTable(name, col_desc)
    true
  end

end

describe Anubis::Table do
  let(:stub_client){ StubClient.new }
  
  before do
    Anubis.connection.stub!(:connected?).and_return(true)
    Anubis.connection.stub!(:safely_send) do |message, *args|
      stub_client.send(message, *args)
    end
  end

  subject{ described_class }
  
  context '.list' do
    it 'lists the tables' do
      subject.list.map(&:name).should eq(['foo', 'bar', 'baz'])
    end
  end

  context '.find' do
    context 'when the table exists' do
      it 'returns the table instance' do
        subject.find('foo').should be_instance_of(Anubis::Table)
        subject.find('foo').name.should eq('foo')
      end
    end

    context 'when the table does not exist' do
      it 'returns nil' do
        subject.find('unreal').should eq(nil)
      end
    end
  end
  
  context '.find_or_create' do
    context 'when the table exists' do
      it 'returns the table instance' do
        existing = subject.find('foo')
        subject.find_or_create('foo').should eq(existing)
      end
    end

    context 'when the table does not exist' do
      it 'creates a new table instance' do
        subject.find('unreal').should eq(nil)
        subject.find_or_create('unreal').tap do |created|
          created.should      be_instance_of(Anubis::Table)
          created.name.should eq('unreal')
        end
      end
    end  
  end
end
