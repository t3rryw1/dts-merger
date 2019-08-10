package com.cozystay.model;

import java.util.List;

public interface DataItemList extends List<DataItem> {

    DataItemList merge(DataItemList list);

    DataItemList diff(DataItemList list);

}
