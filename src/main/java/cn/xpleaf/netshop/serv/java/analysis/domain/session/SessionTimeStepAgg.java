package cn.xpleaf.netshop.serv.java.analysis.domain.session;

import java.io.Serializable;

/**
 * @author xpleaf
 * @date 2018/10/22 下午11:05
 */
public class SessionTimeStepAgg {

    private int td_1s_3s;
    private int td_4s_6s;
    private int td_7s_9s;
    private int td_10s_30s;
    private int td_30s_60s;
    private int td_1m_3m;
    private int td_3m_10m;
    private int td_10m_30m;
    private int td_30m;
    private int sl_1_3;
    private int sl_4_6;
    private int sl_7_9;
    private int sl_10_30;
    private int sl_30_60;
    private int sl_60;
    private int session_count;

    public SessionTimeStepAgg() {

    }

    public void setValue(String key, int value) {
        switch (key) {
            case "td_1s_3s":
                this.td_1s_3s = value;
                break;
            case "td_4s_6s":
                this.td_4s_6s = value;
                break;
            case "td_7s_9s":
                this.td_7s_9s = value;
                break;
            case "td_10s_30s":
                this.td_10s_30s = value;
                break;
            case "td_30s_60s":
                this.td_30s_60s = value;
                break;
            case "td_1m_3m":
                this.td_1m_3m = value;
                break;
            case "td_3m_10m":
                this.td_3m_10m = value;
                break;
            case "td_10m_30m":
                this.td_10m_30m = value;
                break;
            case "td_30m":
                this.td_30m = value;
                break;
            case "sl_1_3":
                this.sl_1_3 = value;
                break;
            case "sl_4_6":
                this.sl_4_6 = value;
                break;
            case "sl_7_9":
                this.sl_7_9 = value;
                break;
            case "sl_10_30":
                this.sl_10_30 = value;
                break;
            case "sl_30_60":
                this.sl_30_60 = value;
                break;
            case "sl_60":
                this.sl_60 = value;
                break;
            case "session_count":
                this.session_count = value;
                break;
        }
    }

    public int getTd_1s_3s() {
        return td_1s_3s;
    }

    public void setTd_1s_3s(int td_1s_3s) {
        this.td_1s_3s = td_1s_3s;
    }

    public int getTd_4s_6s() {
        return td_4s_6s;
    }

    public void setTd_4s_6s(int td_4s_6s) {
        this.td_4s_6s = td_4s_6s;
    }

    public int getTd_7s_9s() {
        return td_7s_9s;
    }

    public void setTd_7s_9s(int td_7s_9s) {
        this.td_7s_9s = td_7s_9s;
    }

    public int getTd_10s_30s() {
        return td_10s_30s;
    }

    public void setTd_10s_30s(int td_10s_30s) {
        this.td_10s_30s = td_10s_30s;
    }

    public int getTd_30s_60s() {
        return td_30s_60s;
    }

    public void setTd_30s_60s(int td_30s_60s) {
        this.td_30s_60s = td_30s_60s;
    }

    public int getTd_1m_3m() {
        return td_1m_3m;
    }

    public void setTd_1m_3m(int td_1m_3m) {
        this.td_1m_3m = td_1m_3m;
    }

    public int getTd_3m_10m() {
        return td_3m_10m;
    }

    public void setTd_3m_10m(int td_3m_10m) {
        this.td_3m_10m = td_3m_10m;
    }

    public int getTd_10m_30m() {
        return td_10m_30m;
    }

    public void setTd_10m_30m(int td_10m_30m) {
        this.td_10m_30m = td_10m_30m;
    }

    public int getTd_30m() {
        return td_30m;
    }

    public void setTd_30m(int td_30m) {
        this.td_30m = td_30m;
    }

    public int getSl_1_3() {
        return sl_1_3;
    }

    public void setSl_1_3(int sl_1_3) {
        this.sl_1_3 = sl_1_3;
    }

    public int getSl_4_6() {
        return sl_4_6;
    }

    public void setSl_4_6(int sl_4_6) {
        this.sl_4_6 = sl_4_6;
    }

    public int getSl_7_9() {
        return sl_7_9;
    }

    public void setSl_7_9(int sl_7_9) {
        this.sl_7_9 = sl_7_9;
    }

    public int getSl_10_30() {
        return sl_10_30;
    }

    public void setSl_10_30(int sl_10_30) {
        this.sl_10_30 = sl_10_30;
    }

    public int getSl_30_60() {
        return sl_30_60;
    }

    public void setSl_30_60(int sl_30_60) {
        this.sl_30_60 = sl_30_60;
    }

    public int getSl_60() {
        return sl_60;
    }

    public void setSl_60(int sl_60) {
        this.sl_60 = sl_60;
    }

    public int getSession_count() {
        return session_count;
    }

    public void setSession_count(int session_count) {
        this.session_count = session_count;
    }

    @Override
    public String toString() {
        return "SessionTimeStepAgg{" +
                "td_1s_3s=" + td_1s_3s +
                ", td_4s_6s=" + td_4s_6s +
                ", td_7s_9s=" + td_7s_9s +
                ", td_10s_30s=" + td_10s_30s +
                ", td_30s_60s=" + td_30s_60s +
                ", td_1m_3m=" + td_1m_3m +
                ", td_3m_10m=" + td_3m_10m +
                ", td_10m_30m=" + td_10m_30m +
                ", td_30m=" + td_30m +
                ", sl_1_3=" + sl_1_3 +
                ", sl_4_6=" + sl_4_6 +
                ", sl_7_9=" + sl_7_9 +
                ", sl_10_30=" + sl_10_30 +
                ", sl_30_60=" + sl_30_60 +
                ", sl_60=" + sl_60 +
                ", session_count=" + session_count +
                '}';
    }
}
