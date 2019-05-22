package com.talentana.bigdata.utils;

public class Constants {

    public enum Gender {male,famale }

    public enum Status {LIVE, DEAD}

    public enum PClass {ONE("1"),TWO("2"),THREE("3");

        private final String value;

        private PClass(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}
