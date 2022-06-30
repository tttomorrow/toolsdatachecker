package org.opengauss.datachecker.check.task;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opengauss.datachecker.check.cache.TableStatusRegister;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class TableStatusRegisterTest {

    private TableStatusRegister tableStatusRegisterUnderTest;

    @BeforeEach
    void setUp() {
        tableStatusRegisterUnderTest = new TableStatusRegister();
        testInit();
    }

    @Test
    void testInit() {
        // Setup
        // Run the test
        tableStatusRegisterUnderTest.init(Set.of("tabel1", "tabel2", "tabel3", "tabel4"));
        // Verify the results
    }

    @Test
    void testPut() {
        tableStatusRegisterUnderTest.put("tabel5", 3);
    }

    @Test
    void testGet() {
        assertThat(tableStatusRegisterUnderTest.get("tabel1")).isEqualTo(0);
    }

    @Test
    void testUpdate() {
        System.out.println("0|1 = " + (0 | 1));
        System.out.println("0|2 = " + (0 | 2));
        System.out.println("1|1 = " + (1 | 1));
        System.out.println("1|2 = " + (1 | 2));
        System.out.println("1|2|4 = " + (1 | 2 | 4));
        System.out.println("4 = " + Integer.toBinaryString(4));
        System.out.println(tableStatusRegisterUnderTest.get("tabel1"));
        assertThat(tableStatusRegisterUnderTest.update("tabel1", 1)).isEqualTo(1);

    }

    @Test
    void testRemove() {
        // Setup
        // Run the test
        tableStatusRegisterUnderTest.remove("key");

        // Verify the results
    }

    @Test
    void testRemoveAll() {
        // Setup
        // Run the test
        tableStatusRegisterUnderTest.removeAll();

        // Verify the results
    }

    /**
     * 线程状态观测
     * Thread.State
     * 线程状态。线程可处于以下状态之一：
     * NEW 尚未启动的线程处于此状态
     * RUNNABLE 在Java虚拟机中执行的线程处于此状态
     * BLOCKED 被阻塞等待监视器锁定的线程处于此状态
     * WAITING 正在等待另一个线程执行特定的动作的线程处于此状态
     * TIMED_WAITING 正在等待另一个线程执行动作达到指定等待时间的线程处于此状态
     * TERMINATED 已退出的线程处于此状态
     *
     * @throws InterruptedException
     */
    @Test
    void testPersistent() throws InterruptedException {
        Thread thread = new Thread(() -> {
            for (int i = 0; i < 5; i++) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("------------");
        });

        Thread.State state = thread.getState();
        System.out.println(state);

        thread.start();
        state = thread.getState();
        System.out.println(state);

        boolean a = true;
        while (a) {
            Thread.sleep(2000);
            System.out.println(thread.getState());

            thread.start();

            System.out.println(thread.getState());
            a = false;
        }
    }
}
