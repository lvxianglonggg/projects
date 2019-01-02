package com.lxl.analysis;

import com.lxl.analysis.tool.AnalysisTool;
import org.apache.hadoop.util.ToolRunner;

public class Bootstrap {

    public static void main(String[] args) {
        try {
            ToolRunner.run(new AnalysisTool(),args);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
