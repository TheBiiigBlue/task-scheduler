# Task-Scheduler
任务调度器，多线程并行调度，支持DAG


## 背景
&emsp;&emsp;我司要做一个在页面上托拉拽进行ETL的操作，类似于kettle，但我们不想用kettle这种C/S架构，我们要做B/S架构，封装为产品。而这种托拉拽图形，并最终形成执行逻辑去执行的过程是整个过程中最复杂的过程。因为我们解析图形内容，解析为可执行的逻辑，并根据依赖关系，并行执行，即在画布上托拉拽的逻辑本身就是个有前后依赖顺序的逻辑关系，我们需要解析为可执行的DAG有向无环图，并根据依赖关系正确的调度任务。

## 调研
&emsp;&emsp;为此我们调研了xxl-job，azkaban两种技术，并进行了对比。
&emsp;&emsp;对于xxl-job，它是一种轻量的任务调度框架，可以很好的植入我们的系统，但是它不支持DAG这种有依赖关系的逻辑，它只支持一个父任务拆分多个子任务，而无法完成多个父任务合并为一个任务，即类似于join的操作，所以xxl-job被pass了。
&emsp;&emsp;对于azkaban，它也是一种轻量的任务调度框架，它也支持DAG，生态也很好，但是经过讨论我们还是抛弃了它，因为它的任务调度总是以zip包的形式进行提交，我们需要把执行逻辑翻译为job文件，打成zip包，通过api提交azkaban，不太友好，而且对我们来说有一些功能不能满足，所以我们最终也是抛弃了它，而选择自研。

## 思路
&emsp;&emsp;通过github同性交友网站，我还是找到一个类似的，简单易懂，易于整合到我们系统的demo版本的调度小框架，地址：[https://github.com/tovin-xu/task-scheduler](https://github.com/tovin-xu/task-scheduler)
&emsp;&emsp;看过这个demo之后，给了我一些启发，我决定在它基础上整改一波，植入我们的系统，因为这个demo还是有很多问题的，譬如：
1. 非springboot版本
2. 角色包括manager、scheduler、executor等，关系混乱
3. 有很多多余的操作，比如阻塞队列缓存完成的任务，其实没有必要
4. 异常没有处理，导致很多线程没有被正确关闭
5. 抽象不够完整
6. 还有等等一些未完善的处理和操作

&emsp;&emsp;在此版本基础上，我对该逻辑进行了整理和完善，主要规整和完善了如下内容：
1. 整合为springboot版本
2. 角色调整和融合，使各角色功能更清晰透明合理  
3. 清除不必要操作，更简洁明了
4. 完好的处理异常、关闭线程等各种优化
5. 对角色和部分进行进一步抽象和提取
6. 增加状态信息，提供对外查询调度进度、状态、耗时等功能
7. 增加日志收集功能，每次调度生成一个日志文件，记录该调度任务执行过程
8. 增加websocket，通过websocket，通过线程池，启动线程，实时读取日志内容，推送前端，实时显示

&emsp;&emsp;到这里已经满足了我们公司的基本需求，或者说主要逻辑、通用逻辑，当然为了适应我司的业务还需要加入很多嘈杂的部分，这部分就不再这里说了，至少这个版本是个通用的版本，如果大家有需要，可以直接将这部分植入系统，几乎可以不用改动，如果后续有优化或者问题，我都会实时更新，欢迎大家讨论，经过完善的github地址：[https://github.com/TheBiiigBlue/task-scheduler.git](https://github.com/TheBiiigBlue/task-scheduler.git)



## 调度逻辑

1. 将所有的NodeTask封装为一个ParentTask，形成DAG有向无环图 ，每个ParentTask起一个线程调度；
2. 首先调度没有依赖的NodeTask，通过线程池提交任务，异步获取运行结果
3. NodeTask运行完成，通过异步回调，并修改NodeTask状态
4. ParentTask 继续调度下面的组件
5. 当该NodeTask所依赖的其他NodeTask都执行完成后，就可以被调度，并发调度
6. 当任一NodeTask执行失败，经过异步回调返回失败，并通知ParentTask调度线程终止调度，并修改任务状态
7. 每次调度，通过动态创建logback  logger，动态生成调度日志文件。
8. 通过websocket实时返回日志信息给页面展示

&emsp;&emsp;完整代码还请参考github地址：[https://github.com/TheBiiigBlue/task-scheduler.git](https://github.com/TheBiiigBlue/task-scheduler.git)
