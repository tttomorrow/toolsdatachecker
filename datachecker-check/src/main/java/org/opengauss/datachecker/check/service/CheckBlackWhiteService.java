package org.opengauss.datachecker.check.service;

import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.check.config.DataCheckProperties;
import org.opengauss.datachecker.common.entry.enums.CheckBlackWhiteMode;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * @author ：wangchao
 * @date ：Created in 2022/6/22
 * @since ：11
 */
@Service
public class CheckBlackWhiteService {
    private static final Set<String> WHITE = new ConcurrentSkipListSet<>();
    private static final Set<String> BLACK = new ConcurrentSkipListSet<>();

    @Autowired
    private FeignClientService feignClientService;

    @Autowired
    private DataCheckProperties dataCheckProperties;

    /**
     * 添加白名单列表 该功能清理历史白名单，重置白名单为当前列表
     *
     * @param whiteList 白名单列表
     */
    public void addWhiteList(List<String> whiteList) {
        WHITE.clear();
        WHITE.addAll(whiteList);
        refushWhiteList();
    }

    /**
     * 更新白名单列表 该功能在当前白名单基础上新增当前列表到白名单
     *
     * @param whiteList 白名单列表
     */
    public void updateWhiteList(List<String> whiteList) {
        WHITE.addAll(whiteList);
        refushWhiteList();
    }

    /**
     * 移除白名单列表 该功能在当前白名单基础上移除当前列表到白名单
     *
     * @param whiteList 白名单列表
     */
    public void deleteWhiteList(List<String> whiteList) {
        WHITE.removeAll(whiteList);
        refushWhiteList();
    }

    /**
     * 查询白名单列表
     *
     * @return 白名单列表
     */
    public List<String> queryWhiteList() {
        return new ArrayList<>(WHITE);
    }

    public void addBlackList(List<String> blackList) {
        BLACK.clear();
        BLACK.addAll(blackList);
        refushWhiteList();
    }

    public void updateBlackList(List<String> blackList) {
        BLACK.addAll(blackList);
        refushWhiteList();
    }

    public void deleteBlackList(List<String> blackList) {
        BLACK.removeAll(blackList);
        refushWhiteList();
    }

    public List<String> queryBlackList() {
        return new ArrayList<>(BLACK);
    }

    private void refushWhiteList() {
        final CheckBlackWhiteMode blackWhiteMode = dataCheckProperties.getBlackWhiteMode();
        if (blackWhiteMode == CheckBlackWhiteMode.WHITE) {
            // 白名单模式
            feignClientService.getClient(Endpoint.SOURCE).refushBlackWhiteList(blackWhiteMode, new ArrayList<>(WHITE));
            feignClientService.getClient(Endpoint.SINK).refushBlackWhiteList(blackWhiteMode, new ArrayList<>(WHITE));
        } else if (blackWhiteMode == CheckBlackWhiteMode.BLACK) {
            // 黑名单模式
            feignClientService.getClient(Endpoint.SOURCE).refushBlackWhiteList(blackWhiteMode, new ArrayList<>(BLACK));
            feignClientService.getClient(Endpoint.SINK).refushBlackWhiteList(blackWhiteMode, new ArrayList<>(BLACK));
        }
    }


}
