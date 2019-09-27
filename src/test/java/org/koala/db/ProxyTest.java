package org.koala.db;

import java.lang.reflect.*;

/**
 * @author shengri
 * @date 9/26/19
 */
public class ProxyTest {
    interface Task {
        String name();
    }

    static class TaskImpl implements Task {

        @Override
        public String name() {
            return "test";
        }
    }

    static class MyInvocationHandler implements InvocationHandler {

        Object task;

        public MyInvocationHandler(Object task) {
            this.task = task;
        }

        @Override
        public Object invoke(Object proxy, Method m, Object[] args) throws Throwable {
            try {
                System.out.println(m.getName());
                return m.invoke(task, args);
            } catch (InvocationTargetException e) {
                throw e;
            } catch (Exception e) {
                throw e;
            }
        }
    }

    public static void main(String[] args) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
//        Task task = (Task) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(),
//                new Class<?>[]{Task.class}, new MyInvocationHandler(new TaskImpl()));
//
//        System.out.println(task.name());

        Class taskClass = Proxy.getProxyClass(Thread.currentThread().getContextClassLoader(),
                new Class<?>[]{Task.class});

       Constructor[] constructors = taskClass.getConstructors();

        Constructor<?> cons = taskClass.getConstructor(new Class<?>[]{InvocationHandler.class});

        Task task0 = (Task) cons.newInstance(new Object[]{new MyInvocationHandler(new TaskImpl())});

        System.out.println(task0.name());

    }
}
