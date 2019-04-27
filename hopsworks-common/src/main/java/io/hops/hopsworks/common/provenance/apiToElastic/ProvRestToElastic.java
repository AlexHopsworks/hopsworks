/*
 * This file is part of Hopsworks
 * Copyright (C) 2018, Logical Clocks AB. All rights reserved
 *
 * Hopsworks is free software: you can redistribute it and/or modify it under the terms of
 * the GNU Affero General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * Hopsworks is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this program.
 * If not, see <https://www.gnu.org/licenses/>.
 */
package io.hops.hopsworks.common.provenance.apiToElastic;

import io.hops.hopsworks.common.provenance.core.Provenance;

public class ProvRestToElastic {
  public static Provenance.MLType mlTypeParser(ProvElasticFields.DocSubType type) {
    switch(type) {
      case DATASET:
        return Provenance.MLType.DATASET;
      case HIVE:
        return Provenance.MLType.HIVE;
      case FEATURE:
      case FEATURE_PART:
        return Provenance.MLType.FEATURE;
      case TRAINING_DATASET:
      case TRAINING_DATASET_PART:
        return Provenance.MLType.TRAINING_DATASET;
      case EXPERIMENT:
      case EXPERIMENT_PART:
        return Provenance.MLType.EXPERIMENT;
      case MODEL:
      case MODEL_PART:
        return Provenance.MLType.MODEL;
      case NONE:
      default:
        return Provenance.MLType.NONE;
    }
  }
}
