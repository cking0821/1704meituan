package com.aura.dao.factory;


import com.aura.dao.ITaskDAO;
import com.aura.dao.impl.TaskDAOImpl;

/**
 * DAO工厂类
 */
public class DAOFactory {

    /**
     * 获取任务管理DAO
     * @return DAO
     */
    public static ITaskDAO getTaskDAO() {
        return new TaskDAOImpl();
    }

}
