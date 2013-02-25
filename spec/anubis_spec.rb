require 'spec_helper'

describe Anubis do
  
  subject{ described_class }
  
  context '.configure' do
    context 'given a block' do
      it 'yields the connection for configuration' do
        subject.connection.should_receive(:host=).with('hostname')
        subject.configure{ |c| c.host = 'hostname' }
      end
      
      it 'resets the thrift protocol' do
        subject.connection.should_receive(:reset_thrift_protocol!)
        subject.configure{ |c| c.host = 'hostname' }
      end
    end
    
    context 'without a block' do
      it 'does not reset the thrift protocol' do
        subject.connection.should_not_receive(:reset_thrift_protocol!)
        subject.configure
      end      
    end

    it 'returns self for chaining' do
      subject.configure{ |c| c.host = 'hostname' }.should eq(subject)
    end    
  end

  context '.connect!' do
    before do
      subject.connection.stub(:connect).and_return(true)
    end

    it 'creates a connection' do
      subject.connection.should_receive(:connect)
      subject.connect!
    end

    context 'in a deploy pack' do
      before do
        subject.stub(:deploy_config).and_return({ host: 'hostname' })
      end

      it 'configures using deploy_config' do
        subject.should_receive(:deploy_pack?).and_return(true)
        subject.connection.should_receive(:host=).with('hostname')
        subject.connect!
      end
    end

    context 'in a rails app' do
      before do
        subject.stub(:rails_config).and_return({ host: 'hostname' })
      end

      it 'configures using rails_config' do
        subject.should_receive(:rails?).and_return(true)
        subject.connection.should_receive(:host=).with('hostname')
        subject.connect!
      end
    end
  end

  context '.tables' do
    
  end

  context 'operations' do
    let(:operation){ test_operation(test_params) }
    
    # This should be made into a proper helper
    def test_operation params
      op = Anubis::Operation.new(params[:table]).
      columns(*params[:columns]).
        qualifier(params[:qualifier]).
        rows(*params[:rows])
      op.stub(:perform).and_return(op)
      op
    end

    context '.get' do
      let(:test_params){ { table: 'my_table' } }
      
      it 'performs a get operation' do
        # subject.should_receive(:operation).with(test_params).and_return(operation)
        # subject.get(test_params).validate.should be(true)
      end
    end
  end
end
