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
  before :all do
    $stdout.sync = true
    with_valid_session
    @user_mail="user"
    @user_pass="pass"
    @feature_name="feature"
    @project_name="project"
  end

  describe "training dataset with features count" do
    it "check training dataset" do 
      create_session(@user_mail, @user_pass)
      project = get_project_by_name(@project_name)
      pp get_ml_asset_by_xattr_count(project, "TRAINING_DATASET", "features.name", "avg_sold_for", 1)
      pp get_ml_td_count_using_feature_project(project, @feature_name)
    end
  end
end