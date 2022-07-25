package org.opengauss.datachecker.extract.constants;

import org.opengauss.datachecker.common.constant.Constants;

public interface ExtConstants {
    String PRIMARY_DELIMITER = Constants.PRIMARY_DELIMITER;
    String DELIMITER = ",";

    /**
     * query result parsing ResultSet data result set,default start index position
     */
    int COLUMN_INDEX_FIRST_ZERO = 0;
}
