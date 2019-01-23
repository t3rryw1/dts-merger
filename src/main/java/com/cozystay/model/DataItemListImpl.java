package com.cozystay.model;

import java.util.ArrayList;
import java.util.List;

public class DataItemListImpl extends ArrayList<DataItem> implements DataItemList {


    public DataItemListImpl() {
        super();
    }

    public DataItemListImpl(DataItemListImpl dataItems) {
        super(dataItems);
    }

    @Override
    public DataItemList merge(DataItemList list) {
        DataItemListImpl newItems = new DataItemListImpl(this);
        List<DataItem> toAddItems = new ArrayList<>();
        nextItem:
        for (DataItem item1 : list) {

            for (DataItem item : newItems) {
                if (item1.equals(item)) {
                    continue nextItem;
                }
                if (item.getId().equals(item1.getId())) {
                    item.merge(item1);
                    item.setUpdateFlag(true);
                    continue nextItem;
                }
            }
            toAddItems.add(item1);
        }
        newItems.addAll(toAddItems);

        return newItems;
    }

    @Override
    public DataItemList diff(DataItemList list) {
        DataItemListImpl newItems = new DataItemListImpl(this);
        newItems.removeAll(list);
        return newItems;

    }
}
