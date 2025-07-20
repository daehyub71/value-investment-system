                       help='결과 파일 출력 경로')
    
    args = parser.parse_args()
    
    # 배치 처리기 초기화
    processor = BuffettBatchProcessor(data_dir=args.data_dir)
    
    try:
        if args.stock_code:
            # 특정 종목 처리
            logger.info(f"특정 종목 분석: {args.stock_code}")
            
            # 종목명 조회
            stock_list = processor.get_stock_list()
            stock_info = next((s for s in stock_list if s['stock_code'] == args.stock_code), None)
            
            if stock_info:
                result = processor.process_single_stock(args.stock_code, stock_info['company_name'])
                if result:
                    print(f"\n✅ 분석 완료: {result.company_name}")
                    print(f"총점: {result.total_score:.1f}/110점")
                    print(f"등급: {result.overall_grade}")
                    print(f"추천: {result.investment_grade.value}")
                else:
                    print(f"❌ 분석 실패: {args.stock_code}")
            else:
                print(f"❌ 종목을 찾을 수 없습니다: {args.stock_code}")
                
        else:
            # 전체 배치 처리
            results = processor.process_all_stocks(limit=args.limit)
            
            if results:
                # 스크리닝 결과 저장
                processor.save_screening_results(args.output)
                
                print(f"\n🎉 배치 처리 완료!")
                print(f"처리된 종목: {len(results)}개")
                print(f"결과 파일: {args.output}")
                
                # 간단한 통계
                scores = [r.total_score for r in results]
                print(f"평균 점수: {sum(scores)/len(scores):.1f}점")
                print(f"최고 점수: {max(scores):.1f}점")
                
                # 상위 5개 종목
                top_5 = sorted(results, key=lambda x: x.total_score, reverse=True)[:5]
                print("\n🏆 상위 5개 종목:")
                for i, result in enumerate(top_5, 1):
                    print(f"  {i}. {result.company_name} ({result.stock_code}) - {result.total_score:.1f}점")
            else:
                print("❌ 처리된 종목이 없습니다.")
                
    except KeyboardInterrupt:
        logger.info("사용자에 의해 중단됨")
        print("\n⏹️ 처리가 중단되었습니다.")
    except Exception as e:
        logger.error(f"실행 중 오류: {e}")
        print(f"❌ 오류 발생: {e}")

if __name__ == "__main__":
    main()
