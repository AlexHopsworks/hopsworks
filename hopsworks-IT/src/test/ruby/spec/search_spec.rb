=begin
 This file is part of Hopsworks
 Copyright (C) 2020, Logical Clocks AB. All rights reserved

 Hopsworks is free software: you can redistribute it and/or modify it under the terms of
 the GNU Affero General Public License as published by the Free Software Foundation,
 either version 3 of the License, or (at your option) any later version.

 Hopsworks is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 PURPOSE.  See the GNU Affero General Public License for more details.

 You should have received a copy of the GNU Affero General Public License along with this program.
 If not, see <https://www.gnu.org/licenses/>.
=end
describe "On #{ENV['OS']}" do
  before(:all) do
    @debugOpt = true
    with_valid_session
  end
  after(:all) do
    # clean_all_test_projects
  end

  def s_create_featuregroup_checked(project, featurestore_id, featuregroup_name)
    json_result, f_name = create_cached_featuregroup(project[:id], featurestore_id, featuregroup_name: featuregroup_name)
    expect_status_details(201)
    parsed_json = JSON.parse(json_result, :symbolize_names => true)
    parsed_json[:id]
  end

  context "global" do
  end

  context "project" do
    before(:all) do
      with_valid_project
    end

    it "search in name and xattr" do
      dataset1_name = "car1"
      dataset1 = create_dataset_by_name_checked(@project, dataset1_name)
      dataset2_name = "car2"
      dataset2 = create_dataset_by_name_checked(@project, dataset2_name)
      dataset3_name = "othername1"
      dataset3 = create_dataset_by_name_checked(@project, dataset3_name)
      dataset4_name = "othername2"
      dataset4 = create_dataset_by_name_checked(@project, dataset4_name)
      add_xattr(@project, get_path_dataset(@project, dataset4), "car", "audi")
      dataset5_name = "othername3"
      dataset5 = create_dataset_by_name_checked(@project, dataset5_name)
      add_xattr(@project, get_path_dataset(@project, dataset5), "hobby", "cars")
      sleep(1)
      time_this do
        wait_for_me(15) do
          result = project_search(@project, "car")
          if result.length == 3
            array_contains_one_of(result) {|r| r[:name] == dataset1_name}
            array_contains_one_of(result) {|r| r[:name] == dataset2_name}
            array_contains_one_of(result) {|r| r[:name] == dataset5_name}
            result_contains_xattr_one_of(result) {|r| r.key?(:hobby) && r[:hobby] == "cars"}
            true
          else
            false
          end
        end
      end
    end
  end

  context "dataset" do
    before(:all) do
      with_valid_project
      with_valid_dataset
    end

    it "search in name and xattr" do
      dir1_name = "car1"
      dir1 = create_dir_checked(@project, get_path_dir(@project, @dataset, dir1_name), "")
      dir2_name = "car2"
      dir2 = create_dir_checked(@project, get_path_dir(@project, @dataset, dir2_name), "")
      dir3_name = "othername1"
      dir3 = create_dir_checked(@project, get_path_dir(@project, @dataset, dir3_name), "")
      dir4_name = "othername2"
      dir4_path = get_path_dir(@project, @dataset, dir4_name)
      dir4 = create_dir_checked(@project, dir4_path, "")
      add_xattr(@project, dir4_path, "car", "audi")
      dir5_name = "othername3"
      dir5_path = get_path_dir(@project, @dataset, dir5_name)
      dir5 = create_dir_checked(@project, dir5_path, "")
      add_xattr(@project, dir5_path, "hobby", "cars")
      sleep(1)
      time_this do
        wait_for_me(15) do
          result = dataset_search(@project, @dataset, "car")
          if result.length == 3
            array_contains_one_of(result) {|r| r[:name] == dir1_name}
            array_contains_one_of(result) {|r| r[:name] == dir2_name}
            array_contains_one_of(result) {|r| r[:name] == dir5_name}
            result_contains_xattr_one_of(result) {|r| r.key?(:hobby) && r[:hobby] == "cars"}
            true
          else
            false
          end
        end
      end
    end
  end

  context "featuregroup dataset" do
    before(:each) do
      with_valid_project
    end

    it "search in name and xattr" do
      featurestore_name = @project[:projectname].downcase + "_featurestore.db"
      featurestore_dataset = get_dataset(@project, featurestore_name)
      featurestore_id = get_featurestore_id(@project[:id])
      featuregroup1_name = "car1"
      featuregroup1_id = s_create_featuregroup_checked(@project, featurestore_id, featuregroup1_name)
      featuregroup2_name = "car2"
      featuregroup2_id = s_create_featuregroup_checked(@project, featurestore_id, featuregroup2_name)
      featuregroup3_name = "othername1"
      featuregroup3_id = s_create_featuregroup_checked(@project, featurestore_id, featuregroup3_name)
      featuregroup4_name = "othername2"
      featuregroup4_id = s_create_featuregroup_checked(@project, featurestore_id, featuregroup4_name)
      add_xattr_featuregroup(@project, featurestore_id, featuregroup4_id, "car", "audi")
      featuregroup5_name = "othername3"
      featuregroup5_id = s_create_featuregroup_checked(@project, featurestore_id, featuregroup5_name)
      add_xattr_featuregroup(@project, featurestore_id, featuregroup5_id, "hobby", "cars")
      sleep(1)
      time_this do
        wait_for_me(15) do
          result = dataset_search(@project, featurestore_dataset, "car")
          if result.length == 3
            array_contains_one_of(result) {|r| r[:name] == "#{featuregroup1_name}_1"}
            array_contains_one_of(result) {|r| r[:name] == "#{featuregroup2_name}_1"}
            array_contains_one_of(result) {|r| r[:name] == "#{featuregroup5_name}_1"}
            result_contains_xattr_one_of(result) {|r| r.key?(:hobby) && r[:hobby] == "cars"}
            true
          else
            pp "received:#{result.length}"
            false
          end
        end
      end
    end

    it "search in name and xattr simple" do
      featurestore_name = @project[:projectname].downcase + "_featurestore.db"
      featurestore_dataset = get_dataset(@project, featurestore_name)
      featurestore_id = get_featurestore_id(@project[:id])
      featuregroup1_name = "car1"
      featuregroup1_id = s_create_featuregroup_checked(@project, featurestore_id, featuregroup1_name)
      featuregroup2_name = "car2"
      featuregroup2_id = s_create_featuregroup_checked(@project, featurestore_id, featuregroup2_name)
      featuregroup3_name = "othername1"
      featuregroup3_id = s_create_featuregroup_checked(@project, featurestore_id, featuregroup3_name)
      featuregroup4_name = "othername2"
      featuregroup4_id = s_create_featuregroup_checked(@project, featurestore_id, featuregroup4_name)
      add_xattr_featuregroup(@project, featurestore_id, featuregroup4_id, "car", "audi")
      featuregroup5_name = "othername3"
      featuregroup5_id = s_create_featuregroup_checked(@project, featurestore_id, featuregroup5_name)
      add_xattr_featuregroup(@project, featurestore_id, featuregroup5_id, "hobby", "cars")
      sleep(10)
      result = dataset_search(@project, featurestore_dataset, "car")
      expect(result.length).to eq(3)
      array_contains_one_of(result) {|r| r[:name] == "#{featuregroup1_name}_1"}
      array_contains_one_of(result) {|r| r[:name] == "#{featuregroup2_name}_1"}
      array_contains_one_of(result) {|r| r[:name] == "#{featuregroup5_name}_1"}
      result_contains_xattr_one_of(result) {|r| r.key?(:hobby) && r[:hobby] == "cars"}
    end
  end

  context "featurestore" do
    before(:each) do
      with_valid_project
    end

    it "search featuregroup in name and xattr" do
      featurestore_name = @project[:projectname].downcase + "_featurestore.db"
      featurestore_id = get_featurestore_id(@project[:id])
      featuregroup1_name = "car1"
      featuregroup1_id = s_create_featuregroup_checked(@project, featurestore_id, featuregroup1_name)
      featuregroup2_name = "car2"
      featuregroup2_id = s_create_featuregroup_checked(@project, featurestore_id, featuregroup2_name)
      featuregroup3_name = "othername1"
      featuregroup3_id = s_create_featuregroup_checked(@project, featurestore_id, featuregroup3_name)
      featuregroup4_name = "othername2"
      featuregroup4_id = s_create_featuregroup_checked(@project, featurestore_id, featuregroup4_name)
      add_xattr_featuregroup(@project, featurestore_id, featuregroup4_id, "car", "audi")
      featuregroup5_name = "othername3"
      featuregroup5_id = s_create_featuregroup_checked(@project, featurestore_id, featuregroup5_name)
      add_xattr_featuregroup(@project, featurestore_id, featuregroup5_id, "hobby", "cars")
      sleep(1)
      time_this do
        wait_for_me(15) do
          result = local_featurestore_search(@project, "FEATUREGROUP", "car")
          if result.length == 3
            array_contains_one_of(result) {|r| r[:name] == "#{featuregroup1_name}"}
            array_contains_one_of(result) {|r| r[:name] == "#{featuregroup2_name}"}
            array_contains_one_of(result) {|r| r[:name] == "#{featuregroup5_name}"}
            result_contains_xattr_one_of(result) {|r| r.key?(:hobby) && r[:hobby] == "cars"}
            true
          else
            pp "received:#{result.length}"
            pp "#{result}"
            false
          end
        end
      end
    end

    it "search training dataset in name and xattr" do
      featurestore_name = @project[:projectname].downcase + "_featurestore.db"
      featurestore_id = get_featurestore_id(@project[:id])
      featuregroup1_name = "car1"
      featuregroup1_id = s_create_featuregroup_checked(@project, featurestore_id, featuregroup1_name)
      featuregroup2_name = "car2"
      featuregroup2_id = s_create_featuregroup_checked(@project, featurestore_id, featuregroup2_name)
      featuregroup3_name = "othername1"
      featuregroup3_id = s_create_featuregroup_checked(@project, featurestore_id, featuregroup3_name)
      featuregroup4_name = "othername2"
      featuregroup4_id = s_create_featuregroup_checked(@project, featurestore_id, featuregroup4_name)
      add_xattr_featuregroup(@project, featurestore_id, featuregroup4_id, "car", "audi")
      featuregroup5_name = "othername3"
      featuregroup5_id = s_create_featuregroup_checked(@project, featurestore_id, featuregroup5_name)
      add_xattr_featuregroup(@project, featurestore_id, featuregroup5_id, "hobby", "cars")
      sleep(1)
      time_this do
        wait_for_me(15) do
          result = local_featurestore_search(@project, "car")
          if result.length == 3
            array_contains_one_of(result) {|r| r[:name] == "#{featuregroup1_name}"}
            array_contains_one_of(result) {|r| r[:name] == "#{featuregroup2_name}"}
            array_contains_one_of(result) {|r| r[:name] == "#{featuregroup5_name}"}
            result_contains_xattr_one_of(result) {|r| r.key?(:hobby) && r[:hobby] == "cars"}
            true
          else
            pp "received:#{result.length}"
            false
          end
        end
      end
    end
  end
end

