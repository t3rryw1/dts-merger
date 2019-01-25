package com.cozystay.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataItemListImpl extends ArrayList<DataItem> implements DataItemList {


    public DataItemListImpl() {
        super();
    }

    public DataItemListImpl(DataItemListImpl dataItems) {
        super(dataItems);
    }

    public DataItemListImpl(DataItem[] dataItems) {
        super(Arrays.asList(dataItems));
    }

    @Override
    public DataItemList merge(DataItemList list) {
        DataItemListImpl mergedItems = new DataItemListImpl(this);
        mergedItems.clear();
        List<DataItem> toAddItems = new ArrayList<>();
        nextItem:
        for (DataItem item1 : list) {

            for (DataItem item : this) {
                if (item1.equals(item)) {
                    mergedItems.add(item);
                    continue nextItem;
                }
                if (item.getIndex().equals(item1.getIndex())) {
                    DataItem merged = item.merge(item1);
                    merged.setUpdateFlag(true);
                    mergedItems.add(merged);
                    continue nextItem;
                }
            }
            toAddItems.add(item1);
        }
        mergedItems.addAll(toAddItems);

        return mergedItems;
    }

    @Override
    public DataItemList diff(DataItemList list) {
        DataItemListImpl newItems = new DataItemListImpl(this);
        newItems.removeAll(list);
        return newItems;

    }
}
