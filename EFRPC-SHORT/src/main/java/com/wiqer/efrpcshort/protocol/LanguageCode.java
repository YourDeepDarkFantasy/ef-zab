package com.wiqer.efrpcshort.protocol;


public enum LanguageCode {
    JAVA((byte) 0),
    GOLANG((byte) 1),
    CSHARP((byte) 2);

    private byte code;

    LanguageCode(byte code) {
        this.code = code;
    }

    public static LanguageCode valueOf(byte code) {
        for (LanguageCode languageCode : LanguageCode.values()) {
            if (languageCode.getCode() == code) {
                return languageCode;
            }
        }
        return null;
    }

    public byte getCode() {
        return code;
    }
}
