package com.pjm.pjm.Controller;


import java.util.regex.Pattern;

public class TestRegex{

    public static void main(String[] args) {

        String strE = "我拒绝的积极性积分";

        String pattern = "[\\u4E00-\\u9FA5]";

        String[] splitStr = strE.split("");

        for(String str:splitStr) {

            if(Pattern.matches(pattern, strE))

                System.out.println(strE);
        }

    }

}



