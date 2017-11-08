package com.hbase.tools.hfile.beans;

import org.apache.hadoop.io.Text;

/**
 * Created by hzxijingjing on 16/7/12.
 */
public class UserInfoExportBean implements InterfaceExportBean {
    private String key;
    private String ssn;
    private String real_name;
    private String id_card_qualified;
    private String gender;
    private String region;
    private String province;
    private String county;
    private String birthday;
    private String reg_time;
    private String reg_ip;
    private String update_pwd_time;
    private String bind_mobile_time;
    private String login_timestamp;
    private String bind_phone;
    private String bind_qq ;
    private String bind_email;
    private String bind_otp;
    private String bind_otp_phone;
    private String bind_otp_real;
    private String bind_ppc;
    private String security_code;
    private String mibao_question;
    private String mob_mail;
    private String mob_account;
    private String gen1_gen2;
    private String game_vip;
    private String epay_user;
    private String game_user;
    private String locked;
    private String login_province_times;
    private String login_province;
    private String leak_flag_hacker;
    private String otp_risk;
    private String leak_flag_lpc;
    private String leak_flag_xff;
    private String freq_login_province;
    private String leak_flag_product;
    private String mark_leak_time;
    private String clear_leak_time;
    private String is_suspicious_spam;
    private String is_mass_creation;
    private String last_complaint_ts;

    public void setKey(String key) {
        this.key = key;
    }

    public void setSsn(String ssn) {
        this.ssn = ssn;
    }

    public void setReal_name(String real_name) {
        this.real_name = real_name;
    }

    public void setId_card_qualified(String id_card_qualified) {
        this.id_card_qualified = id_card_qualified;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public void setCounty(String county) {
        this.county = county;
    }

    public void setBirthday(String birthday) {
        this.birthday = birthday;
    }

    public void setReg_time(String reg_time) {
        this.reg_time = reg_time;
    }

    public void setReg_ip(String reg_ip) {
        this.reg_ip = reg_ip;
    }

    public void setUpdate_pwd_time(String update_pwd_time) {
        this.update_pwd_time = update_pwd_time;
    }

    public void setBind_mobile_time(String bind_mobile_time) {
        this.bind_mobile_time = bind_mobile_time;
    }

    public void setLogin_timestamp(String login_timestamp) {
        this.login_timestamp = login_timestamp;
    }

    private void setBind_phone(String bind_phone) {
        this.bind_phone = bind_phone;
    }

    public void setBind_qq(String bind_qq) {
        this.bind_qq = bind_qq;
    }

    public void setBind_email(String bind_email) {
        this.bind_email = bind_email;
    }

    public void setBind_otp(String bind_otp) {
        this.bind_otp = bind_otp;
    }

    public void setBind_otp_phone(String bind_otp_phone) {
        this.bind_otp_phone = bind_otp_phone;
    }

    public void setBind_otp_real(String bind_otp_real) {
        this.bind_otp_real = bind_otp_real;
    }

    public void setBind_ppc(String bind_ppc) {
        this.bind_ppc = bind_ppc;
    }

    public void setSecurity_code(String security_code) {
        this.security_code = security_code;
    }

    public void setMibao_question(String mibao_question) {
        this.mibao_question = mibao_question;
    }

    public void setMob_mail(String mob_mail) {
        this.mob_mail = mob_mail;
    }

    public void setMob_account(String mob_account) {
        this.mob_account = mob_account;
    }

    public void setGen1_gen2(String gen1_gen2) {
        this.gen1_gen2 = gen1_gen2;
    }

    public void setGame_vip(String game_vip) {
        this.game_vip = game_vip;
    }

    public void setEpay_user(String epay_user) {
        this.epay_user = epay_user;
    }

    public void setGame_user(String game_user) {
        this.game_user = game_user;
    }

    public void setLocked(String locked) {
        this.locked = locked;
    }

    public void setLogin_province_times(String login_province_times) {
        this.login_province_times = login_province_times;
    }

    public void setLogin_province(String login_province) {
        this.login_province = login_province;
    }

    public void setLeak_flag_hacker(String leak_flag_hacker) {
        this.leak_flag_hacker = leak_flag_hacker;
    }

    public void setOtp_risk(String otp_risk) {
        this.otp_risk = otp_risk;
    }

    public void setLeak_flag_lpc(String leak_flag_lpc) {
        this.leak_flag_lpc = leak_flag_lpc;
    }

    public void setLeak_flag_xff(String leak_flag_xff) {
        this.leak_flag_xff = leak_flag_xff;
    }

    public void setFreq_login_province(String freq_login_province) {
        this.freq_login_province = freq_login_province;
    }

    public void setLeak_flag_product(String leak_flag_product) {
        this.leak_flag_product = leak_flag_product;
    }

    public void setMark_leak_time(String mark_leak_time) {
        this.mark_leak_time = mark_leak_time;
    }

    public void setClear_leak_time(String clear_leak_time) {
        this.clear_leak_time = clear_leak_time;
    }

    public void setIs_suspicious_spam(String is_suspicious_spam) {
        this.is_suspicious_spam = is_suspicious_spam;
    }

    public void setIs_mass_creation(String is_mass_creation) {
        this.is_mass_creation = is_mass_creation;
    }

    public void setLast_complaint_ts(String last_complaint_ts) {
        this.last_complaint_ts = last_complaint_ts;
    }

    @Override
    public String toString() {
        String str = key + "," +
                ssn + "," +
                real_name + "," +
                id_card_qualified + "," +
                gender + "," +
                region + "," +
                province + "," +
                county + "," +
                birthday + "," +
                reg_time + "," +
                reg_ip + "," +
                update_pwd_time + "," +
                bind_mobile_time + "," +
                login_timestamp + "," +
                bind_phone + "," +
                bind_qq + "," +
                bind_email + "," +
                bind_otp + "," +
                bind_otp_phone + "," +
                bind_otp_real + "," +
                bind_ppc + "," +
                security_code + "," +
                mibao_question + "," +
                mob_mail + "," +
                mob_account + "," +
                gen1_gen2 + "," +
                game_vip + "," +
                epay_user + "," +
                game_user + "," +
                locked + "," +
                login_province_times + "," +
                login_province + "," +
                leak_flag_hacker + "," +
                otp_risk + "," +
                leak_flag_lpc + "," +
                leak_flag_xff + "," +
                freq_login_province + "," +
                leak_flag_product + "," +
                mark_leak_time + "," +
                clear_leak_time + "," +
                is_suspicious_spam + "," +
                is_mass_creation + "," +
                last_complaint_ts ;
        if(str.isEmpty()  || str == null)
            return "";
        else
            return str;
    }

    public void setBean(Iterable<Text> values) {
        for (Text value : values) {
            String str = value.toString();
            String col = str.split("=")[0];
            String val = str.split("=")[1];
            if (col.equals("key")){
                setKey(val);
            }
            if (col.equals("ssn")){
                setSsn(val);
            }
            if (col.equals("real_name")){
                setReal_name(val);
            }
            if(col.equals("id_card_qualified")) {
                setId_card_qualified(val);
            }
            if (col.equals("gender")){
                setGender(val);
            }
            if (col.equals("region")){
                setRegion(val);
            }
            if (col.equals("province")){
                setProvince(val);
            }
            if(col.equals("county")) {
                setCounty(val);
            }
            if (col.equals("birthday")){
                setBirthday(val);
            }
            if (col.equals("reg_time")){
                setReg_time(val);
            }
            if (col.equals("reg_ip")){
                setReg_ip(val);
            }
            if(col.equals("update_pwd_time")) {
                setUpdate_pwd_time(val);
            }
            if (col.equals("bind_mobile_time")){
                setBind_mobile_time(val);
            }
            if (col.equals("login_timestamp")){
                setLogin_timestamp(val);
            }
            if (col.equals("bind_phone")){
                setBind_phone(val);
            }
            if(col.equals("bind_qq")) {
                setBind_qq(val);
            }
            if (col.equals("bind_email")){
                setBind_email(val);
            }
            if (col.equals("bind_otp")){
                setBind_otp(val);
            }
            if (col.equals("bind_otp_phone")){
                setBind_otp_phone(val);
            }
            if(col.equals("bind_otp_real")) {
                setBind_otp_real(val);
            }
            if (col.equals("bind_ppc")){
                setBind_ppc(val);
            }
            if (col.equals("security_code")){
                setSecurity_code(val);
            }
            if (col.equals("mibao_question")){
                setMibao_question(val);
            }
            if(col.equals("mob_mail")) {
                setMob_mail(val);
            }
            if (col.equals("mob_account")){
                setMob_account(val);
            }
            if (col.equals("gen1_gen2")){
                setGen1_gen2(val);
            }
            if (col.equals("game_vip")){
                setGame_vip(val);
            }
            if(col.equals("epay_user")) {
                setEpay_user(val);
            }
            if (col.equals("game_user")){
                setGame_user(val);
            }
            if (col.equals("locked")){
                setLocked(val);
            }
            if (col.equals("login_province_times")){
                setLogin_province_times(val);
            }
            if(col.equals("login_province")) {
                setLogin_province(val);
            }
            if (col.equals("leak_flag_hacker")){
                setLeak_flag_hacker(val);
            }
            if (col.equals("otp_risk")){
                setOtp_risk(val);
            }
            if (col.equals("leak_flag_lpc")){
                setLeak_flag_lpc(val);
            }
            if(col.equals("leak_flag_xff")) {
                setLeak_flag_xff(val);
            }
            if (col.equals("freq_login_province")){
                setFreq_login_province(val);
            }
            if (col.equals("leak_flag_product")){
                setLeak_flag_product(val);
            }
            if (col.equals("mark_leak_time")){
                setMark_leak_time(val);
            }
            if(col.equals("clear_leak_time")) {
                setClear_leak_time(val);
            }
            if (col.equals("is_suspicious_spam")){
                setIs_suspicious_spam(val);
            }
            if (col.equals("is_mass_creation")){
                setIs_mass_creation(val);
            }
            if (col.equals("last_complaint_ts")){
                setLast_complaint_ts(val);
            }
        }
    }
}
