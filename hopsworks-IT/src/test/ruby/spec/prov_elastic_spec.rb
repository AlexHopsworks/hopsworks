=begin
 This file is part of Hopsworks
 Copyright (C) 2018, Logical Clocks AB. All rights reserved

 Hopsworks is free software: you can redistribute it and/or modify it under the terms of
 the GNU Affero General Public License as published by the Free Software Foundation,
 either version 3 of the License, or (at your option) any later version.

 Hopsworks is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 PURPOSE.  See the GNU Affero General Public License for more details.

 You should have received a copy of the GNU Affero General Public License along with this program.
 If not, see <https://www.gnu.org/licenses/>.
=end

require 'pp'

describe "On #{ENV['OS']}" do
  describe 'provenance integration tests' do
    def check_index(index_name)
      Airborne.configure do |config|
        config.base_url = ''
      end
      head "#{ENV['ELASTIC_API']}/#{index_name}"
      expect_status(200)
    end

    def get_entry(index_name, key)
      Airborne.configure do |config|
        config.base_url = ''
      end
      query_string = '{"query": {"ids" : {"values" : ["' + key + '"] }}}'
      query = JSON.parse(query_string)
      target = "#{ENV['ELASTIC_API']}/#{index_name}/_search"
      result = post target, query
      expect_status(200)
      parsed_result = JSON.parse(result)
    end

    def check_file_create(key, iName, mlType) 
      puts "key #{key}"
      index_name = "fileprovenance"
      parsed_result = get_entry(index_name, key)
      entry = parsed_result["hits"]["hits"][0]["_source"]
      expect(entry["i_name"]).to eq iName
      expect(entry["inode_operation"]).to eq "CREATE"
      expect(entry["mlType"]).to eq mlType
    end
    
    def check_file_entry(key, opName, mlType) 
      puts "key #{key}"
      index_name = "fileprovenance"
      parsed_result = get_entry(index_name, key)
      entry = parsed_result["hits"]["hits"][0]["_source"]
      expect(entry["inode_operation"]).to eq opName
    end

    def check_xattr_entry(key) 
      puts "key #{key}"
      index_name = "fileprovenance"
      parsed_result = get_entry(index_name, key)
      entry = parsed_result["hits"]["hits"][0]["_source"]
      expect(entry["inode_operation"]).to eq "XATTR_ADD"
    end

    def check_ml_state(key, mlType) 
      puts "key #{key}"
      index_name = "fileprovenance"
      parsed_result = get_entry(index_name, key)
      entry = parsed_result["hits"]["hits"][0]["_source"]
      expect(entry["mlType"]).to eq mlType
    end

    it "should find the fileprovenance index" do
      check_index("fileprovenance")
    end

    it "should find the appprovenance index" do
      check_index("appprovenance")
    end

    it "should find all entries (up to 100)" do
      Airborne.configure do |config|
         config.base_url = ''
      end
      index_name="fileprovenance"
      
      result = get "#{ENV['ELASTIC_API']}/#{index_name}/_search?size=100&q=*:*"
      expect_status(200)
      parsed_result = JSON.parse(result)
      expect(parsed_result['hits']['total']).to eq 20
    end

    it "check keys" do
      check_file_create("2-CREATE-2-2--1", "project_1", "none")
      check_file_create("3-CREATE-3-3--1", "dataset_1", "none")
      check_file_create("4-CREATE-4-4--1", "Models", "none")
      check_file_create("5-CREATE-5-5--1", "project_1_Training_Datasets", "none")
      check_file_create("6-CREATE-6-6--1", "project_1_featurestore.db", "none")
      check_file_create("7-CREATE-7-7--1", "Experiments", "none")
      check_file_create("30-CREATE-30-30--1", "dir1", "none")
      check_file_create("40-CREATE-40-40--1", "Model_A", "none")
      check_file_create("41-CREATE-41-41--1", "V_1", "model")
      check_file_create("42-CREATE-42-42--1", "Model_A_V_1_File", "model_part")
      check_file_create("50-CREATE-50-50--1", "TD_A_V_1", "training_dataset")
      check_file_create("51-CREATE-51-51--1", "TD_A_V_1_File", "training_dataset_part")
      check_file_create("60-CREATE-60-60--1", "FG_A_V_1", "feature")
      check_file_create("61-CREATE-61-61--1", "FG_A_V_1_File", "feature_part")
      check_file_create("70-CREATE-70-70--1", "Experiment_A_V_1", "experiment")
      check_file_create("71-CREATE-71-71--1", "Experiment_A_V_1_File", "experiment_part")
      check_file_entry("30-DELETE-31-31--1", "DELETE", "none")
      check_ml_state("70", "experiment")
      check_xattr_entry("70-XATTR_ADD-100-100--1")
      check_xattr_entry("70-XATTR_ADD-101-101--1")
    end
  end
end