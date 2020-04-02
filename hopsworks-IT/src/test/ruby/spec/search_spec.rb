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
    clean_all_test_projects
  end

  def s_create_featuregroup_checked(project, featurestore_id, featuregroup_name)
    pp "create featuregroup:#{featuregroup_name}" if defined?(@debugOpt) && @debugOpt == true
    json_result, f_name = create_cached_featuregroup(project[:id], featurestore_id, featuregroup_name: featuregroup_name)
    expect_status_details(201)
    parsed_json = JSON.parse(json_result, :symbolize_names => true)
    parsed_json[:id]
  end

  def s_create_featuregroup_checked2(project, featurestore_id, featuregroup_name, features)
    pp "create featuregroup:#{featuregroup_name}" if defined?(@debugOpt) && @debugOpt == true
    json_result, f_name = create_cached_featuregroup(project[:id], featurestore_id, featuregroup_name: featuregroup_name, features:features)
    expect_status_details(201)
    parsed_json = JSON.parse(json_result, :symbolize_names => true)
    parsed_json[:id]
  end

  def s_create_training_dataset_checked(project, featurestore_id, connector, training_dataset_name)
    pp "create training dataset:#{training_dataset_name}" if defined?(@debugOpt) && @debugOpt == true
    json_result, training_dataset_name_aux = create_hopsfs_training_dataset(project.id, featurestore_id, connector, name:training_dataset_name)
    expect_status_details(201)
    parsed_json = JSON.parse(json_result, :symbolize_names => true)
    parsed_json
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
    end

    def featuregroups_setup(project)
      featurestore_id = get_featurestore_id(project[:id])

      fg1_name = "fg_animal1"
      featuregroup1_id = s_create_featuregroup_checked(project, featurestore_id, fg1_name)
      fg2_name = "fg_dog1"
      featuregroup2_id = s_create_featuregroup_checked(project, featurestore_id, fg2_name)
      fg3_name = "fg_othername1"
      featuregroup3_id = s_create_featuregroup_checked(project, featurestore_id, fg3_name)
      fg4_name = "fg_othername2"
      features4 = [
          {
              type: "INT",
              name: "dog",
              description: "--",
              primary: true
          }
      ]
      featuregroup4_id = s_create_featuregroup_checked2(project, featurestore_id, fg4_name, features4)
      fg5_name = "fg_othername3"
      features5 = [
          {
              type: "INT",
              name: "cat",
              description: "--",
              primary: true
          }
      ]
      featuregroup5_id = s_create_featuregroup_checked2(project, featurestore_id, fg5_name, features5)
      add_xattr_featuregroup(project, featurestore_id, featuregroup5_id, "hobby", "tennis")
      fg6_name = "fg_othername6"
      featuregroup6_id = s_create_featuregroup_checked(project, featurestore_id, fg6_name)
      add_xattr_featuregroup(project, featurestore_id, featuregroup6_id, "animal", "dog")
      fg7_name = "fg_othername7"
      featuregroup7_id = s_create_featuregroup_checked(project, featurestore_id, fg7_name)
      fg7_tags = [{'key' => "dog", 'value' => "Luna"}, {'key' => "other", 'value' => "val"}]
      add_xattr_featuregroup(project, featurestore_id, featuregroup7_id, "tags", fg7_tags.to_json)
      fg8_name = "fg_othername8"
      featuregroup8_id = s_create_featuregroup_checked(project, featurestore_id, fg8_name)
      fg8_tags = [{'key' => "pet", 'value' => "dog"}, {'key' => "other", 'value' => "val"}]
      add_xattr_featuregroup(project, featurestore_id, featuregroup8_id, "tags", fg8_tags.to_json)
      fg9_name = "fg_othername9"
      featuregroup9_id = s_create_featuregroup_checked(project, featurestore_id, fg9_name)
      fg9_tags = [{'key' => "dog"}, {'key' => "other", 'value' => "val"}]
      add_xattr_featuregroup(project, featurestore_id, featuregroup9_id, "tags", fg9_tags.to_json)
      [fg1_name, fg2_name, fg3_name, fg4_name, fg5_name, fg6_name, fg7_name, fg8_name, fg9_name]
    end

    def trainingdataset_setup(project)
      featurestore_id = get_featurestore_id(project[:id])

      td_name = "#{project[:projectname]}_Training_Datasets"
      td_dataset = get_dataset(project, td_name)
      connector = get_hopsfs_training_datasets_connector(project[:projectname])
      td1_name = "td_animal1"
      td1 = s_create_training_dataset_checked(project, featurestore_id, connector, td1_name)
      td2_name = "td_dog1"
      td2 = s_create_training_dataset_checked(project, featurestore_id, connector, td2_name)
      td3_name = "td_something1"
      td3 = s_create_training_dataset_checked(project, featurestore_id, connector, td3_name)
      add_xattr(project, get_path_dir(project, td_dataset, "#{td3_name}_#{td3[:version]}"), "td_key", "dog_td")
      td4_name = "td_something2"
      td4 = s_create_training_dataset_checked(project, featurestore_id, connector, td4_name)
      add_xattr(project, get_path_dir(project, td_dataset, "#{td4_name}_#{td4[:version]}"), "td_key", "something_val")
      #fake features
      td5_name = "td_something5"
      td5 = s_create_training_dataset_checked(project, featurestore_id, connector, td5_name)
      td5_features = [{'featurestore_id' => 100, 'name' => "dog", 'version' => 1, 'features' => ["feature1", "feature2"]}]
      add_xattr(project, get_path_dir(project, td_dataset, "#{td5_name}_#{td5[:version]}"), "featurestore.td_features", td5_features.to_json)
      td6_name = "td_something6"
      td6 = s_create_training_dataset_checked(project, featurestore_id, connector, td6_name)
      td6_features = [{'featurestore_id' => 100, 'name' => "fg", 'version' => 1, 'features' => ["dog", "feature2"]}]
      add_xattr(project, get_path_dir(project, td_dataset, "#{td6_name}_#{td6[:version]}"), "featurestore.td_features", td6_features.to_json)
      [td1_name, td2_name, td3_name, td4_name, td5_name, td6_name]
    end

    def check_searched(result, name, project_name, highlights)
      result[:name] == name && result[:parentProjectName] == project_name &&
          defined?(result[:highlights].key(highlights))
    end

    def check_searched_feature(result, featuregroup, project_name)
      result[:featuregroup] == featuregroup && result[:parentProjectName] == project_name &&
          defined?(result[:highlights].key("name"))
    end

    it "search featuregroup, training datasets with name, features, xattr" do
      project1 = create_project
      project2 = create_project
      fgs1 = featuregroups_setup(project1)
      fgs2 = featuregroups_setup(project2)
      tds1 = trainingdataset_setup(project1)
      tds2 = trainingdataset_setup(project2)
      #local search - project1
      sleep(1)
      time_this do
        wait_for_me(15) do
          result = local_featurestore_search(project1, "FEATUREGROUP", "dog")
          pp result
          r_aux = result[:featuregroups].length == 6
          r_aux = r_aux && check_array_contains_one_of(result[:featuregroups]) {|r|
            check_searched(r, fgs1[1], project1[:projectname], "name")}
          r_aux = r_aux && check_array_contains_one_of(result[:featuregroups]) {|r|
            check_searched(r, fgs1[3], project1[:projectname], "features")}
          r_aux = r_aux && check_array_contains_one_of(result[:featuregroups]) {|r|
            check_searched(r, fgs1[5], project1[:projectname], "otherXattrs")}
          r_aux = r_aux && check_array_contains_one_of(result[:featuregroups]) {|r|
            check_searched(r, fgs1[6], project1[:projectname], "tags")}
          r_aux = r_aux && check_array_contains_one_of(result[:featuregroups]) {|r|
            check_searched(r, fgs1[7], project1[:projectname], "tags")}
          r_aux = r_aux && check_array_contains_one_of(result[:featuregroups]) {|r|
            check_searched(r, fgs1[8], project1[:projectname], "tags")}
          r_aux
        end
      end
      sleep(1)
      time_this do
        wait_for_me(15) do
          result = local_featurestore_search(project1, "TRAININGDATASET", "dog")
          pp result
          r_aux = result[:trainingdatasets].length == 4
          r_aux = r_aux && check_array_contains_one_of(result[:trainingdatasets]) {|r|
            check_searched(r, tds1[1], project1[:projectname], "name")}
          r_aux = r_aux && check_array_contains_one_of(result[:trainingdatasets]) {|r|
            check_searched(r, tds1[2], project1[:projectname], "name")}
          r_aux = r_aux && check_array_contains_one_of(result[:trainingdatasets]) {|r|
            check_searched(r, tds1[4], project1[:projectname], "features")}
          r_aux = r_aux && check_array_contains_one_of(result[:trainingdatasets]) {|r|
            check_searched(r, tds1[5], project1[:projectname], "features")}
          r_aux
        end
      end
      sleep(1)
      time_this do
        wait_for_me(15) do
          result = local_featurestore_search(project1, "FEATURE", "dog")
          pp result
          r_aux = result[:features].length == 1
          r_aux = r_aux && check_array_contains_one_of(result[:features]) {|r|
            check_searched_feature(r, fgs1[3], project1[:projectname])}
          r_aux
        end
      end
      #global search
      sleep(1)
      time_this do
        wait_for_me(15) do
          result = global_featurestore_search("FEATUREGROUP", "dog")
          pp result
          r_aux = true
          if result[:featuregroups].length >= 12
            r_aux = r_aux && check_array_contains_one_of(result[:featuregroups]) {|r|
              check_searched(r, fgs1[1], project1[:projectname], "name")}
            r_aux = r_aux && check_array_contains_one_of(result[:featuregroups]) {|r|
              check_searched(r, fgs1[3], project1[:projectname], "features")}
            r_aux = r_aux && check_array_contains_one_of(result[:featuregroups]) {|r|
              check_searched(r, fgs1[5], project1[:projectname], "otherXattrs")}
            r_aux = r_aux && check_array_contains_one_of(result[:featuregroups]) {|r|
              check_searched(r, fgs1[6], project1[:projectname], "tags")}
            r_aux = r_aux && check_array_contains_one_of(result[:featuregroups]) {|r|
              check_searched(r, fgs1[7], project1[:projectname], "tags")}
            r_aux = r_aux && check_array_contains_one_of(result[:featuregroups]) {|r|
              check_searched(r, fgs1[8], project1[:projectname], "tags")}

            r_aux = r_aux && check_array_contains_one_of(result[:featuregroups]) {|r|
              check_searched(r, fgs2[1], project2[:projectname], "name")}
            r_aux = r_aux && check_array_contains_one_of(result[:featuregroups]) {|r|
              check_searched(r, fgs2[3], project2[:projectname], "features")}
            r_aux = r_aux && check_array_contains_one_of(result[:featuregroups]) {|r|
              check_searched(r, fgs2[5], project2[:projectname], "otherXattrs")}
            r_aux = r_aux && check_array_contains_one_of(result[:featuregroups]) {|r|
              check_searched(r, fgs2[6], project2[:projectname], "tags")}
            r_aux = r_aux && check_array_contains_one_of(result[:featuregroups]) {|r|
              check_searched(r, fgs2[7], project2[:projectname], "tags")}
            r_aux = r_aux && check_array_contains_one_of(result[:featuregroups]) {|r|
              check_searched(r, fgs2[8], project2[:projectname], "tags")}
            expect(r_aux).to eq(true), "global search - hard to get exact results with contaminated index}"
            true
          else
            false
          end
        end
      end
      sleep(1)
      time_this do
        wait_for_me(15) do
          result = global_featurestore_search("TRAININGDATASET", "dog")
          pp result
          r_aux = true
          if result[:trainingdatasets].length >= 8
            r_aux = r_aux && check_array_contains_one_of(result[:trainingdatasets]) {|r|
              check_searched(r, tds1[1], project1[:projectname], "name")}
            r_aux = r_aux && check_array_contains_one_of(result[:trainingdatasets]) {|r|
              check_searched(r, tds1[2], project1[:projectname], "name")}
            r_aux = r_aux && check_array_contains_one_of(result[:trainingdatasets]) {|r|
              check_searched(r, tds1[4], project1[:projectname], "features")}
            r_aux = r_aux && check_array_contains_one_of(result[:trainingdatasets]) {|r|
              check_searched(r, tds1[5], project1[:projectname], "features")}

            r_aux = r_aux && check_array_contains_one_of(result[:trainingdatasets]) {|r|
              check_searched(r, tds2[1], project2[:projectname], "name")}
            r_aux = r_aux && check_array_contains_one_of(result[:trainingdatasets]) {|r|
              check_searched(r, tds2[2], project2[:projectname], "name")}
            r_aux = r_aux && check_array_contains_one_of(result[:trainingdatasets]) {|r|
              check_searched(r, tds2[4], project2[:projectname], "features")}
            r_aux = r_aux && check_array_contains_one_of(result[:trainingdatasets]) {|r|
              check_searched(r, tds2[5], project2[:projectname], "features")}
            r_aux
            expect(r_aux).to eq(true), "global search - hard to get exact results with contaminated index}"
            true
          else
            false
          end
        end
      end
      sleep(1)
      time_this do
        wait_for_me(15) do
          result = global_featurestore_search("FEATURE", "dog")
          pp result
          r_aux = true
          if result[:features].length >= 2
            r_aux = r_aux && check_array_contains_one_of(result[:features]) {|r|
              check_searched_feature(r, fgs1[3], project1[:projectname])}

            r_aux = r_aux && check_array_contains_one_of(result[:features]) {|r|
              check_searched_feature(r, fgs2[3], project2[:projectname])}
            expect(r_aux).to eq(true), "global search - hard to get exact results with contaminated index}"
            true
          else
            false
          end
        end
      end
    end
  end
end

