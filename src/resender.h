#ifndef PS_RESENDER_H_
#define PS_RESENDER_H_

namespace ps {

/**
 * \brief resend a messsage if no ack is received within a given time
 */
class Resender {

 public:
  /**
   * \param timeout timeout in millisecond
   */
  Resender(int timeout, int num_retry, Van* van);

  ~Resender() {

  }


  /**
   * \brief add an outgoining message
   *
   */
  void AddOutgoing(const Message& msg) {

  }

  /**
   * \brief add an incomming message
   * \brief return true if msg has been added before
   */
  bool AddIncomming(const Message& msg) {
    return false;
  }
};
} // namespace ps
#endif  // RESENDER_H_
