package com.cozystay.notify;

import net.sf.json.JSONObject;

import java.io.IOException;

public interface NotifyAction {
    JSONObject sendRequest () throws IOException;
}
