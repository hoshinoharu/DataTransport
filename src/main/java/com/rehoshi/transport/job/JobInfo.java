package com.rehoshi.transport.job;

import com.google.gson.annotations.SerializedName;

public class JobInfo {
    @SerializedName("Name")
    public String name;
    @SerializedName("BatchSize")
    public int batchSize = 3000;
}
